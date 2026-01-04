using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Port;
using MessageBroker.Inbound.Adapter;
using MessageBroker.Outbound.Adapter;
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
    private readonly FrameMessageUseCase _frameMessageUseCase = new(new MessageFramer());
    private readonly ReceivePublishResponseUseCase _receivePublishResponseUseCase = new(new MessageDeframer());
    private readonly PublishResponseHandler _responseHandler = new();
    private PipeWriter? _pipeWriter;
    private PipeReader? _pipeReader;
    private Task? _processChannelTask;
    private Task? _receiveResponseTask;

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
        _receiveResponseTask = Task.Run(ReceiveResponsesAsync, _cancellationSource.Token);
    }

    public async Task DisconnectAsync()
    {
        await _cancellationSource.CancelAsync();

        try
        {
            await Task.WhenAll(ProcessChannelTask, _receiveResponseTask ?? Task.CompletedTask);

            await PipeWriter.FlushAsync();
            await PipeWriter.CompleteAsync();

            if (_pipeReader != null)
            {
                await _pipeReader.CompleteAsync();
            }

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
            var stream = _client.GetStream();
            _pipeWriter = PipeWriter.Create(stream);
            _pipeReader = PipeReader.Create(stream);

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
        var count = batchMessagesUseCase.Count;
        if (count == 0)
        {
            Logger.LogWarning("Attempted to send empty batch - skipping");
            return;
        }
        
        var batchBytes = batchMessagesUseCase.Build();
        batchMessagesUseCase.Clear();
        
        if (batchBytes == null || batchBytes.Length == 0)
        {
            Logger.LogError($"Built batch is empty or null! Count was: {count}");
            return;
        }
        
        if (batchBytes.Length < 38)
        {
            Logger.LogWarning($"⚠️  Built batch is too small: {batchBytes.Length} bytes (expected at least 38). Count: {count}");
        }
        
        Logger.LogInfo($"📤 Building batch: {count} messages, batch size: {batchBytes.Length} bytes");

        var attempts = 0;
        var sent = false;

        while (!sent && attempts < options.MaxSendAttempts && !_cancellationSource.Token.IsCancellationRequested)
        {
            attempts++;

            try
            {
                await _frameMessageUseCase.WriteFramedMessageAsync(
                    PipeWriter,
                    options.Topic,
                    batchBytes,
                    _cancellationSource.Token);

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
                Logger.LogDebug(
                    $"Sent batch with {count} records, topic: {options.Topic}, batch size: {batchBytes.Length} bytes");
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

    private async Task ReceiveResponsesAsync()
    {
        if (_pipeReader == null)
        {
            return;
        }

        try
        {
            while (!_cancellationSource.Token.IsCancellationRequested)
            {
                var result = await _pipeReader.ReadAsync(_cancellationSource.Token);
                var buffer = result.Buffer;

                ProcessResponsesFromBuffer(ref buffer);

                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Response receiving cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Error receiving responses: {ex.Message}", ex);
        }
    }

    private void ProcessResponsesFromBuffer(ref ReadOnlySequence<byte> buffer)
    {
        while (_receivePublishResponseUseCase.TryReceiveResponse(ref buffer, out var response))
        {
            if (response != null)
            {
                _responseHandler.Handle(response);
            }
        }
    }
}