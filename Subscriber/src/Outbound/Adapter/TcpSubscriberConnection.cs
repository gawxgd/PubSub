using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net.Sockets;
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
    Channel<byte[]> requestChannel,
    Channel<byte[]> responseChannel)
    : ISubscriberConnection, IAsyncDisposable
{
    private readonly TcpClient _client = new();
    private readonly CancellationTokenSource _cancellationSource = new();
    private PipeReader? _pipeReader;
    private PipeWriter? _pipeWriter;
    private Task? _readLoopTask;
    private Task? _writeLoopTask;

    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<TcpSubscriberConnection>(LogSource.Subscriber);

    public async Task ConnectAsync()
    {
        try
        {
            await _client.ConnectAsync(host, port);
            var stream = _client.GetStream();
            _pipeReader = PipeReader.Create(stream);
            _pipeWriter = PipeWriter.Create(stream);

            _readLoopTask = Task.Run(() => ReadLoopAsync(_cancellationSource.Token));
            _writeLoopTask = Task.Run(() => WriteLoopAsync(_cancellationSource.Token));
            Logger.LogInfo($"Connected to broker at {_client.Client.RemoteEndPoint}");
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Connection cancelled.");
            throw;
        }
        catch (SocketException ex)
        {
            Logger.LogError($"Error during connection: {ex.Message}");
            bool isRetriable = IsRetriable(ex);

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
                    $"Socket already closed or disconnected while shutting down connection: {ex.SocketErrorCode}"
                );
            }
            finally
            {
                _client.Close();
            }

            await _cancellationSource.CancelAsync();

            if (_writeLoopTask != null)
                await _writeLoopTask;

            if (_readLoopTask != null)
                await _readLoopTask;

            if (_pipeReader != null)
                await _pipeReader.CompleteAsync();

            if (_pipeWriter != null)
                await _pipeWriter.CompleteAsync();

            responseChannel.Writer.TryComplete();
            requestChannel.Writer.TryComplete();

            Logger.LogInfo($"Disconnected from broker at {_client.Client.RemoteEndPoint}");
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
            Logger.LogInfo("Read loop started - waiting for batches from broker");
            var batchesReceived = 0;
            
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await _pipeReader!.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                Logger.LogDebug($"Read {buffer.Length} bytes from broker");

                while (TryReadBatchMessage(ref buffer, out var batchBytes))
                {
                    batchesReceived++;
                    Logger.LogInfo($"📥 Received batch #{batchesReceived} from broker: {batchBytes.Length} bytes");
                    await responseChannel.Writer.WriteAsync(batchBytes, cancellationToken);
                }

                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted || result.IsCanceled)
                {
                    Logger.LogInfo($"Disconnected from broker at {_client.Client.RemoteEndPoint} (received {batchesReceived} batches total)");
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Read loop cancelled");
        }
        catch (IOException ex) when (ex.InnerException is SocketException socketEx)
        {
            var isRetriable = IsRetriable(socketEx);
            throw new SubscriberConnectionException($"Read loop failed, retriable connection {isRetriable}", socketEx,
                isRetriable);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Unexpected error in read loop: {ex.Message}");
        }
    }

    private bool TryReadBatchMessage(ref ReadOnlySequence<byte> buffer, out byte[] batchBytes)
    {
        //ToDO FIXX DO NOT DELET THIS COMMENT
        batchBytes = Array.Empty<byte>();

        // Need at least 24 bytes: 8 for baseOffset + 4 for batchLength + 8 for lastOffset + 4 for recordBytesLength
        const int minHeaderSize = 24;
        if (buffer.Length < minHeaderSize)
            return false;

        // Read header to get batchLength and recordBytesLength
        // Use BinaryPrimitives to match MessageBroker's LittleEndian encoding
        Span<byte> headerSpan = stackalloc byte[minHeaderSize];
        buffer.Slice(0, minHeaderSize).CopyTo(headerSpan);

        var baseOffset = BinaryPrimitives.ReadUInt64LittleEndian(headerSpan.Slice(0, 8));
        var batchLength = BinaryPrimitives.ReadUInt32LittleEndian(headerSpan.Slice(8, 4));
        var lastOffset = BinaryPrimitives.ReadUInt64LittleEndian(headerSpan.Slice(12, 8));
        var recordBytesLength = BinaryPrimitives.ReadUInt32LittleEndian(headerSpan.Slice(20, 4));
        
        // Debug logging
        Logger.LogDebug($"Reading batch header: baseOffset={baseOffset}, batchLength={batchLength}, lastOffset={lastOffset}, recordBytesLength={recordBytesLength}, bufferLength={buffer.Length}");
        
        // Validate batchLength to avoid reading invalid data
        if (batchLength == 0)
        {
            // If batchLength is 0, it might mean:
            // 1. Empty batch (should not happen, but handle gracefully)
            // 2. Wrong data alignment (maybe we're reading from wrong position)
            // 3. End of stream or padding
            Logger.LogWarning($"batchLength is 0 (baseOffset={baseOffset}, lastOffset={lastOffset}, recordBytesLength={recordBytesLength}), bufferLength={buffer.Length}. First 24 bytes hex: {Convert.ToHexString(headerSpan)}");
            
            // If all header values are 0, this might be padding or end of data
            if (baseOffset == 0 && lastOffset == 0 && recordBytesLength == 0)
            {
                Logger.LogInfo("All header values are 0, likely end of data or padding. Skipping.");
                if (buffer.Length >= minHeaderSize)
                {
                    buffer = buffer.Slice(minHeaderSize);
                }
                return false;
            }
            
            // Otherwise, try to skip just the header and continue
            if (buffer.Length >= minHeaderSize)
            {
                buffer = buffer.Slice(minHeaderSize);
            }
            return false;
        }
        
        if (batchLength > int.MaxValue)
        {
            Logger.LogWarning($"batchLength {batchLength} exceeds int.MaxValue, skipping batch");
            return false;
        }
        // recordBytesLength is at offset 20, but we don't need to read it for size calculation

        // Calculate total batch size: header (20 bytes) + RecordBytesLength field (4 bytes) + batchLength
        // batchLength already includes: MagicNumber + CRC + CompressedFlag + Timestamp + RecordBytes
        // But RecordBytesLength field (4 bytes) is NOT included in batchLength
        const int headerSize = 20; // BaseOffset + BatchLength + LastOffset
        var totalBatchSize = headerSize + sizeof(uint) + (int)batchLength;

        // Check if we have the full batch
        if (buffer.Length < totalBatchSize)
            return false;

        // Extract full batch bytes
        batchBytes = buffer.Slice(0, totalBatchSize).ToArray();
        buffer = buffer.Slice(totalBatchSize);

        Logger.LogInfo(
            $"Received batch: baseOffset={baseOffset}, lastOffset={lastOffset}, batchLength={batchLength} bytes");
        return true;
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in requestChannel.Reader.ReadAllAsync(cancellationToken))
            {
                var writer = _pipeWriter!;
                var span = writer.GetSpan(message.Length);
                message.CopyTo(span);
                writer.Advance(message.Length);
                await writer.FlushAsync(cancellationToken);
                Logger.LogDebug($"Sent request to broker: {message.Length} bytes");
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Write loop cancelled");
        }
        finally
        {
            if (_pipeWriter != null)
                await _pipeWriter.CompleteAsync();
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