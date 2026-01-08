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
    private TcpClient? _client;
    private NetworkStream? _stream;
    private readonly FrameMessageUseCase _frameMessageUseCase = new(new MessageFramer());
    private readonly ReceivePublishResponseUseCase _receivePublishResponseUseCase = new(new MessageDeframer());
    private readonly PublishResponseHandler _responseHandler = new();
    private PipeWriter? _pipeWriter;
    private PipeReader? _pipeReader;
    private Task? _processChannelTask;
    private Task? _receiveResponseTask;
    
    private readonly SemaphoreSlim _reconnectLock = new(1, 1);
    private const int ReconnectDelayMs = 500;
    private const int MaxReconnectAttempts = 10;
    
    private volatile bool _connectionBroken;
    
    public PublishResponseHandler ResponseHandler => _responseHandler;

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
        try
        {
            Logger.LogInfo("Disconnecting from broker...");
            
            await _cancellationSource.CancelAsync();

            // Close socket FIRST to unblock any pending I/O operations
            CloseSocketResources();
            
            // Clear pipe references (don't await completion - can block)
            _pipeWriter = null;
            _pipeReader = null;

            // Don't wait for tasks - they will exit on their own due to cancellation/socket closure
            _processChannelTask = null;
            _receiveResponseTask = null;
            
            deadLetterChannel.Writer.TryComplete();
            _reconnectLock.Dispose();
            _cancellationSource.Dispose();
            
            Logger.LogInfo("Disconnected from broker");
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
            CloseSocketResources();
            
            _client = new TcpClient();
            await _client.ConnectAsync(
                options.MessageBrokerConnectionUri.Host,
                options.MessageBrokerConnectionUri.Port,
                _cancellationSource.Token);
            _stream = _client.GetStream();
            _pipeWriter = PipeWriter.Create(_stream);
            _pipeReader = PipeReader.Create(_stream);

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
    
    private void CloseSocketResources()
    {
        if (_pipeWriter != null)
        {
            try { _pipeWriter.Complete(); }
            catch { /* ignore */ }
            _pipeWriter = null;
        }
        
        if (_pipeReader != null)
        {
            try { _pipeReader.Complete(); }
            catch { /* ignore */ }
            _pipeReader = null;
        }
        
        if (_stream != null)
        {
            try { _stream.Dispose(); }
            catch { /* ignore */ }
            _stream = null;
        }
        
        if (_client != null)
        {
            try
            {
                if (_client.Connected)
                    _client.Client.Shutdown(SocketShutdown.Both);
            }
            catch { /* ignore */ }
            finally
            {
                try { _client.Close(); }
                catch { /* ignore */ }
                _client = null;
            }
        }
    }
    
    private async Task<bool> ReconnectAsync()
    {
        if (_cancellationSource.Token.IsCancellationRequested)
            return false;
        
        await _reconnectLock.WaitAsync(_cancellationSource.Token);
        
        try
        {
            if (!_connectionBroken)
            {
                Logger.LogDebug("Connection already restored by another task");
                return true;
            }
            
            Logger.LogInfo("Attempting to reconnect to broker...");
            
            for (var attempt = 1; attempt <= MaxReconnectAttempts; attempt++)
            {
                if (_cancellationSource.Token.IsCancellationRequested)
                    return false;
                    
                try
                {
                    await Task.Delay(ReconnectDelayMs, _cancellationSource.Token);
                    await HandleConnectionToBroker();
                    
                    _connectionBroken = false;
                    
                    _receiveResponseTask = Task.Run(ReceiveResponsesAsync, _cancellationSource.Token);
                    
                    Logger.LogInfo("Successfully reconnected to broker");
                    return true;
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
                catch (Exception ex)
                {
                    Logger.LogWarning($"Reconnect attempt {attempt}/{MaxReconnectAttempts} failed: {ex.Message}");
                    if (attempt == MaxReconnectAttempts)
                    {
                        Logger.LogError("Max reconnect attempts reached");
                        return false;
                    }
                }
            }
            
            return false;
        }
        finally
        {
            _reconnectLock.Release();
        }
    }

    private async Task ProcessChannelAsync()
    {
        try
        {
            await foreach (var msg in channelReader.ReadAllAsync(_cancellationSource.Token))
            {
                batchMessagesUseCase.Add(msg);

                if (batchMessagesUseCase.ShouldFlush(options.BatchMaxBytes, options.BatchMaxDelay))
                {
                    try
                    {
                        await SendBatchAsync();
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError($"Error sending batch in process loop: {ex.Message}", ex);
                    }
                }
            }

            if (batchMessagesUseCase.Count > 0)
            {
                try
                {
                    await SendBatchAsync();
                }
                catch (Exception ex)
                {
                    Logger.LogError($"Error sending final batch: {ex.Message}", ex);
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Channel processing cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Unexpected error in process channel: {ex.Message}", ex);
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
                if (_connectionBroken)
                {
                    Logger.LogWarning("Connection is marked as broken, attempting to reconnect...");
                    if (!await ReconnectAsync())
                    {
                        Logger.LogError("Failed to reconnect from broken state, cannot send batch");
                        break;
                    }
                }
                
                var writer = _pipeWriter;
                if (writer == null)
                {
                    Logger.LogWarning("No active connection, attempting to reconnect...");
                    if (!await ReconnectAsync())
                    {
                        Logger.LogError("Failed to reconnect, cannot send batch");
                        break;
                    }
                    writer = _pipeWriter;
                    if (writer == null)
                    {
                        Logger.LogError("PipeWriter still null after reconnect");
                        break;
                    }
                }
                
                await _frameMessageUseCase.WriteFramedMessageAsync(
                    writer,
                    options.Topic,
                    batchBytes,
                    _cancellationSource.Token);

                var result = await writer.FlushAsync(_cancellationSource.Token);

                if (result.IsCompleted)
                {
                    Logger.LogWarning("Connection completed (likely broker restart), attempting to reconnect...");
                    if (await ReconnectAsync())
                    {
                        continue; // Retry sending with new connection
                    }
                    break;
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
                Logger.LogWarning($"IO exception: {ex.Message}, attempting to reconnect...", ex);
                if (!await ReconnectAsync())
                {
                    break;
                }
                // Continue to retry with new connection
            }
            catch (SocketException ex) when (CanRetrySocketException(ex))
            {
                Logger.LogWarning($"Retriable socket exception: {ex.Message}, attempting to reconnect...", ex);
                if (!await ReconnectAsync())
                {
                    break;
                }
                // Continue to retry with new connection
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("PipeWriter"))
            {
                Logger.LogWarning("PipeWriter not initialized, attempting to reconnect...");
                if (!await ReconnectAsync())
                {
                    break;
                }
                // Continue to retry with new connection
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
                    Logger.LogWarning("Connection closed by broker, marking connection as broken");
                    _connectionBroken = true;
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
            _connectionBroken = true;
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