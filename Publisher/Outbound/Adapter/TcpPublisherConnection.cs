using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Publisher.Domain.Port;
using Publisher.Outbound.Exceptions;

namespace Publisher.Outbound.Adapter;

//ToDo what should be a case for reconnection creating a new connection instance and what not
public sealed class TcpPublisherConnection(
    string host,
    int port,
    string topic,
    uint maxSendAttempts,
    int batchMaxBytes,
    TimeSpan batchMaxDelay,
    ChannelReader<byte[]> channelReader,
    Channel<byte[]> deadLetterChannel)
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
            // clean shutdown
            await ProcessChannelTask;

            await PipeWriter.FlushAsync();
            await PipeWriter.CompleteAsync();

            _client.Client.Shutdown(SocketShutdown.Both);
            _client.Close();

            deadLetterChannel.Writer.TryComplete();
            // dispose
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
            await _client.ConnectAsync(host, port, _cancellationSource.Token);
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
        var batch = new List<byte[]>(128);
        var currentBatchBytes = 0;
        var lastFlush = DateTime.UtcNow;

        try
        {
            await foreach (var msg in channelReader.ReadAllAsync(_cancellationSource.Token))
            {
                batch.Add(msg);
                currentBatchBytes += msg.Length;

                var now = DateTime.UtcNow;

                var full = currentBatchBytes >= batchMaxBytes;
                var timeout = now - lastFlush >= batchMaxDelay;

                if (full || timeout)
                {
                    await FlushBatchAsync(batch);
                    batch.Clear();
                    currentBatchBytes = 0;
                    lastFlush = now;
                }
            }

            if (batch.Count > 0)
                await FlushBatchAsync(batch);
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("ProcessChannelAsync cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogError($"ProcessChannelAsync failed: {ex.Message}", ex);
        }

        Logger.LogInfo("Finished processing channel");
    }

    private async Task FlushBatchAsync(List<byte[]> messages)
    {
        if (messages.Count == 0)
            return;

        var batch = BuildBatchBytes(topic, messages);

        await SendWithRetries(batch);
    }

    /// <summary>
    /// Makes a batch out of serialized messages.
    /// Adds a size prefix to each message and the whole batch itself
    /// Also adds a topic at the beginning of the batch
    /// [batchContentSize][topicSize][topic][size1][message1][size2][message2]...
    /// where batchContentSize refers only to this part: [size1][message1][size2][message2]...
    /// </summary>
    private byte[] BuildBatchBytes(string topic, List<byte[]> messages)
    {
        var topicBytes = Encoding.UTF8.GetBytes(topic);
        
        int batchContentSize =
            sizeof(int) + topicBytes.Length + messages.Sum(m => sizeof(int) + m.Length);
        
        var result = new byte[sizeof(int) + batchContentSize];
        int offset = 0;
        
        // [batchContentSize]
        BitConverter.GetBytes(batchContentSize).CopyTo(result, offset);
        offset += sizeof(int);

        // [topicSize]
        BitConverter.GetBytes((ushort)topicBytes.Length).CopyTo(result, offset);
        offset += sizeof(ushort);

        // [topic]
        Buffer.BlockCopy(topicBytes, 0, result, offset, topicBytes.Length);
        offset += topicBytes.Length;
        
        //[size1][message1][size2][message2]...
        foreach (var msg in messages)
        {
            BitConverter.GetBytes(msg.Length).CopyTo(result, offset);
            offset += sizeof(int);

            Buffer.BlockCopy(msg, 0, result, offset, msg.Length);
            offset += msg.Length;
        }

        return result;
    }


    private async Task SendWithRetries(byte[] payload)
    {
        var attempts = 0;
        var sent = false;

        while (!sent && attempts < maxSendAttempts && !_cancellationSource.Token.IsCancellationRequested)
        {
            attempts++;

            try
            {
                var mem = PipeWriter.GetMemory(payload.Length);
                payload.AsSpan().CopyTo(mem.Span);
                PipeWriter.Advance(payload.Length);

                var result = await PipeWriter.FlushAsync(_cancellationSource.Token);

                if (result.IsCompleted)
                    throw new PublisherConnectionException("Connection to broker failed");

                if (result.IsCanceled)
                {
                    Logger.LogWarning("Cancellation requested – batch unsent");
                    break;
                }

                sent = true;
            }
            catch (IOException)
            {
                throw new PublisherConnectionException("Connection to broker failed");
            }
            catch (SocketException ex) when (CanRetrySocketException(ex))
            {
                throw new PublisherConnectionException("Connection to broker failed");
            }
        }

        if (!sent)
            SendToDeadLetterNoBlocking(payload);
    }


    private void SendToDeadLetterNoBlocking(byte[] msg)
    {
        // TODO: wychodzi, że to będzie zawieralo cale batche - czy to mamy rozbuc na message?
        if (!deadLetterChannel.Writer.TryWrite(msg))
        {
            Logger.LogWarning("Dead-letter queue full, newest message dropped.");
            return;
        }

        Logger.LogInfo($"Send {msg} to dead letter queue");
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