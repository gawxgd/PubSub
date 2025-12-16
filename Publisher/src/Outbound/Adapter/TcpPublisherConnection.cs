using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Publisher.Configuration.Options;
using Publisher.Domain.Logic;
using Publisher.Domain.Port;
using Publisher.Outbound.Exceptions;

namespace Publisher.Outbound.Adapter;

public sealed class TcpPublisherConnection(
    PublisherOptions options,
    ChannelReader<byte[]> channelReader,
    Channel<byte[]> deadLetterChannel,
    BatchMessagesUseCase batchMessagesUseCase)
    : IPublisherConnection, IAsyncDisposable
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<TcpPublisherConnection>(LogSource.Publisher);

    private readonly CancellationTokenSource _cancellationSource = new();
    private readonly TcpClient _client = new();
    private PipeWriter? _pipeWriter;
    private Task? _processChannelTask;

    private PipeWriter PipeWriter =>
        _pipeWriter ?? throw new InvalidOperationException("PipeWriter has not been initialized.");

    private Task ProcessChannelTask =>
        _processChannelTask ?? throw new InvalidOperationException("Process channel task has not been initialized.");

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
    }

    public async Task ConnectAsync()
    {
        await HandleConnectionToBroker();
        _processChannelTask = Task.Run(ProcessChannelAsync, _cancellationSource.Token);
    }

    public async Task DisconnectAsync()
    {
        await _cancellationSource.CancelAsync();

        try
        {
            await ProcessChannelTask;

            await PipeWriter.FlushAsync();
            await PipeWriter.CompleteAsync();

            _client.Client.Shutdown(SocketShutdown.Both);
            _client.Close();

            deadLetterChannel.Writer.TryComplete();

            _client.Dispose();
            _cancellationSource.Dispose();
        }
        catch (Exception ex)
        {
            Logger.LogError("Exception while disconnecting", ex);
        }
    }

    private async Task HandleConnectionToBroker()
    {
        try
        {
            await _client.ConnectAsync(
                options.MessageBrokerConnectionUri.Host,
                options.MessageBrokerConnectionUri.Port,
                _cancellationSource.Token);
            _pipeWriter = PipeWriter.Create(_client.GetStream());

            Logger.LogInfo($"Connected to broker on {_client.Client.RemoteEndPoint}");
        }
        catch (SocketException ex) when (CanRetrySocketException(ex))
        {
            throw new PublisherConnectionException("Connection to broker failed, but can be retried", ex);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogError($"Unrecoverable connection error: {ex.Message}", ex);
            throw new PublisherException($"Unrecoverable connection error {ex.Message}", ex);
        }
    }

    private async Task ProcessChannelAsync()
    {
        await foreach (var msg in channelReader.ReadAllAsync(_cancellationSource.Token))
        {
            batchMessagesUseCase.Add(msg);

            if (batchMessagesUseCase.ShouldFlush(options.BatchMaxBytes, options.BatchMaxDelay))
            {
                await SendBatchAsync();
            }
        }

        if (batchMessagesUseCase.Count > 0)
        {
            await SendBatchAsync();
        }

        Logger.LogInfo("Finished processing channel");
    }

    private async Task SendBatchAsync()
    {
        var batchBytes = batchMessagesUseCase.Build();
        var count = batchMessagesUseCase.Count;
        batchMessagesUseCase.Clear();

        var attempts = 0;
        var sent = false;

        while (!sent && attempts < options.MaxSendAttempts && !_cancellationSource.Token.IsCancellationRequested)
        {
            attempts++;

            try
            {
                var buffer = PipeWriter.GetMemory(batchBytes.Length);
                batchBytes.CopyTo(buffer);
                PipeWriter.Advance(batchBytes.Length);

                var result = await PipeWriter.FlushAsync(_cancellationSource.Token);

                if (result.IsCompleted)
                {
                    throw new PublisherConnectionException("Connection to broker failed");
                }

                if (result.IsCanceled)
                {
                    Logger.LogWarning(
                        "The cancellation was requested. Batch was not sent, moving to dead letter queue");
                    break;
                }

                sent = true;
                Logger.LogDebug($"Sent batch with {count} records");
            }
            catch (IOException ex)
            {
                Logger.LogWarning($"IO exception: {ex.Message} retrying connection", ex);
                throw new PublisherConnectionException("Connection to broker failed");
            }
            catch (SocketException ex) when (CanRetrySocketException(ex))
            {
                Logger.LogWarning($"Retriable socket exception: {ex.Message} retrying connection", ex);
                throw new PublisherConnectionException("Connection to broker failed");
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogError($"Unexpected error while sending batch: {ex.Message}", ex);
                break;
            }
        }

        if (!sent)
        {
            SendToDeadLetterNoBlocking(batchBytes);
        }
    }

    private void SendToDeadLetterNoBlocking(byte[] msg)
    {
        if (!deadLetterChannel.Writer.TryWrite(msg))
        {
            Logger.LogWarning("Dead-letter queue full, newest message dropped.");
            return;
        }

        Logger.LogInfo("Sent batch to dead letter queue");
    }

    private static bool CanRetrySocketException(SocketException ex)
    {
        return ex.SocketErrorCode switch
        {
            SocketError.TimedOut or
                SocketError.NetworkDown or
                SocketError.NetworkUnreachable or
                SocketError.HostUnreachable or
                SocketError.Interrupted or
                SocketError.ConnectionAborted or
                SocketError.ConnectionReset or
                SocketError.ProcessLimit or
                SocketError.SystemNotReady or
                SocketError.TryAgain => true,
            _ => false
        };
    }
}