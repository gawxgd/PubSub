using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Domain;
using Subscriber.Outbound.Exceptions;

namespace Subscriber.Outbound.Adapter;

public sealed class TcpSubscriberConnection(
    string host,
    int port,
    ChannelWriter<byte[]> messageChannelWriter)
    : ISubscriberConnection, IAsyncDisposable
{
    private readonly TcpClient _client = new();
    private readonly CancellationTokenSource _cancellationSource = new();
    private PipeReader? _pipeReader;
    private Task? _readLoopTask;
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpSubscriberConnection>(LogSource.MessageBroker);

    public async Task ConnectAsync()
    {
        try
        {
            await _client.ConnectAsync(host, port);
            _pipeReader = PipeReader.Create(_client.GetStream());
            _readLoopTask = Task.Run(() => ReadLoopAsync(_cancellationSource.Token), _cancellationSource.Token);
            Logger.LogInfo($"Connected to broker at {_client.Client.RemoteEndPoint}");
        }
        catch (OperationCanceledException ex)
        {
            Logger.LogInfo("Read loop cancelled.");
            throw;
        }
        catch (SocketException ex)
        {
            Logger.LogError( $"Error during connection: {ex.Message}");
            bool isRetriable = this.IsRetriable(ex);

            throw new SubscriberConnectionException("TCP connection failed", ex, isRetriable);
        }
    }

    public async Task DisconnectAsync()
    {
        try
        {
            try
            {
                _client.Client.Shutdown(SocketShutdown.Both);
            }
            catch (SocketException ex)
            {
                Logger.LogInfo(
                    $"Socket already closed or disconnected while shutting down connection : {ex.SocketErrorCode}"
                );
            }
            finally
            {
                _client.Close(); 
            }
            
            await _cancellationSource.CancelAsync();

            if (_readLoopTask != null)
                await _readLoopTask;

            if (_pipeReader != null)
                await _pipeReader.CompleteAsync();
            
            messageChannelWriter.TryComplete();
            
            Logger.LogInfo( $"Disconnected from broker at {_client.Client.RemoteEndPoint}");
            
        }
        catch (Exception ex)
        {
            Logger.LogError($"Error during disconnect: {ex.Message}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await _pipeReader!.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                while (TryReadBatch(ref buffer, out var batch))
                {
                    foreach (var message in UnpackBatch(batch))
                    {
                        await messageChannelWriter.WriteAsync(message, cancellationToken);
                    }
                }

                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted || result.IsCanceled)
                {
                    Logger.LogInfo($"Disconnected from broker at {_client.Client.RemoteEndPoint}");
                    break;
                }
            }
        }
        catch (IOException ex) when (ex.InnerException is SocketException socketEx)
        {
            bool isRetriable = IsRetriable(socketEx);

            throw new SubscriberConnectionException("Read loop failed", socketEx, isRetriable);
        }
        catch (Exception ex)
        {
            throw new SubscriberConnectionException("Unexpected error in read loop", null, false);
        }
    }

    
    /// <summary>Isolates a single batch from the buffer.</summary>
    private bool TryReadBatch(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> batch)
    {
        batch = default;
        
        // Minimum: contentSize + topicSize
        if (buffer.Length < 2 * sizeof(int))
        {
            return false;
        }
        
        var reader = new SequenceReader<byte>(buffer);
        
        if (!reader.TryReadBigEndian(out int batchContentSize))
        {
            return false;
        }
        
        if (!reader.TryReadBigEndian(out int topicSize))
        {
            return false;
        }
        
        long totalBatchSize = 2 * sizeof(int) + topicSize + batchContentSize;
    
        if (buffer.Length < totalBatchSize)
        {
            // We don't have the whole batch yet
            return false;
        }
        
        batch = buffer.Slice(0, totalBatchSize);
        
        buffer = buffer.Slice(totalBatchSize);
        
        Logger.LogDebug($"Read batch: contentSize={batchContentSize}, topicSize={topicSize}, totalSize={totalBatchSize}");

        return true;
    }
    
    /// <summary>Takes the batch apart</summary>
    /// <returns>messages serialized with avro as byte[] still wrapped with their SchemaId but already without
    /// their size prefixes</returns>
    private IEnumerable<byte[]> UnpackBatch(ReadOnlySequence<byte> batch)
    {
        var reader = new SequenceReader<byte>(batch);
        
        reader.Advance(4); //Skip batchContentSize
        
        if (!reader.TryReadBigEndian(out int topicSize))
        {
            Logger.LogError("Failed to read topicSize");
            yield break;
        }
        
        Span<byte> topicBytes = stackalloc byte[topicSize];
        if (!reader.TryCopyTo(topicBytes))
        {
            Logger.LogError("Failed to read topic");
            yield break;
        }
        reader.Advance(topicSize);
        
        var topicName = Encoding.UTF8.GetString(topicBytes);
        Logger.LogDebug($"Unpacking batch for topic: {topicName}");
        
        // Read messages right now they look like [msgSize][schemaId][message]
        while (reader.Remaining > 0)
        {
            if (!reader.TryReadBigEndian(out int messageSize))
            {
                Logger.LogError("Failed to read message size");
                yield break;
            }
            
            if (messageSize <= 0 || messageSize > reader.Remaining)
            {
                Logger.LogError($"Invalid message size: {messageSize}, remaining: {reader.Remaining}");
                yield break;
            }
            
            var messageBytes = new byte[messageSize];
            if (!reader.TryCopyTo(messageBytes))
            {
                Logger.LogError("Failed to read message content");
                yield break;
            }
            reader.Advance(messageSize);
            
            yield return messageBytes;
        }
    }

    private bool IsRetriable(SocketException ex)
    {
        return ex.SocketErrorCode switch
        {
            SocketError.TimedOut => true,
            SocketError.ConnectionRefused => true,
            SocketError.NetworkDown => true,
            SocketError.HostNotFound => true,
            _ => false
        };
    }
}
