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
    private TcpClient? _client;
    private NetworkStream? _stream;
    private PipeReader? _pipeReader;
    private PipeWriter? _pipeWriter;

    // Jeden CTS na całe życie obiektu (stop przy Disconnect/Dispose)
    private readonly CancellationTokenSource _cts = new();

    // Read/Write loop uruchamiane raz, żyją cały czas
    private Task? _readLoopTask;
    private Task? _writeLoopTask;

    private readonly SemaphoreSlim _reconnectLock = new(1, 1);
    private const int ReconnectDelayMs = 1000;

    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<TcpSubscriberConnection>(LogSource.Subscriber);

    private CancellationToken Token => _cts.Token;

    public async Task ConnectAsync()
    {
        // Pierwsze połączenie lub manualne wywołanie – ustawia połączenie i odpala pętle, jeśli jeszcze nie żyją
        try
        {
            await EstablishConnectionAsync().ConfigureAwait(false);

            // Pętle odpalamy tylko raz
            if (_readLoopTask == null)
                _readLoopTask = Task.Run(() => ReadLoopAsync(Token), Token);

            if (_writeLoopTask == null)
                _writeLoopTask = Task.Run(() => WriteLoopAsync(Token), Token);
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Connect cancelled.");
            throw;
        }
        catch (SocketException ex)
        {
            Logger.LogError($"Error during initial connection: {ex.Message}");
            bool isRetriable = IsRetriable(ex);
            throw new SubscriberConnectionException("TCP connection failed", ex, isRetriable);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Unexpected error during initial connection: {ex.Message}");
            throw;
        }
    }

    private async Task EstablishConnectionAsync()
    {
        // Tworzy nowego TcpClient, stream i PipeReader/PipeWriter.
        // Wywoływane przy pierwszym Connect oraz z ReconnectAsync.
        // Nie rusza pętli ani CTS.
        CloseSocketResources();

        _client = new TcpClient();
        Logger.LogInfo($"Connecting to broker at {host}:{port}...");
        await _client.ConnectAsync(host, port).ConfigureAwait(false);

        _stream = _client.GetStream();
        _pipeReader = PipeReader.Create(_stream);
        _pipeWriter = PipeWriter.Create(_stream);

        var remoteEndPoint = _client.Client.RemoteEndPoint?.ToString() ?? $"{host}:{port}";
        Logger.LogInfo($"Connected to broker at {remoteEndPoint}");
    }

    public async Task DisconnectAsync()
    {
        // Twarde zamknięcie – zatrzymuje pętle i połączenie
        try
        {
            Logger.LogInfo("Disconnecting from broker...");

            _cts.Cancel();

            if (_writeLoopTask != null)
            {
                try { await _writeLoopTask.ConfigureAwait(false); }
                catch (Exception ex) { Logger.LogDebug($"Error waiting for write loop: {ex.Message}"); }
                _writeLoopTask = null;
            }

            if (_readLoopTask != null)
            {
                try { await _readLoopTask.ConfigureAwait(false); }
                catch (Exception ex) { Logger.LogDebug($"Error waiting for read loop: {ex.Message}"); }
                _readLoopTask = null;
            }

            await CompletePipesAsync().ConfigureAwait(false);
            CloseSocketResources();

            Logger.LogInfo("Disconnected from broker");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Error during disconnect: {ex.Message}");
        }
    }

    public async Task DisconnectAndCloseChannelsAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        responseChannel.Writer.TryComplete();
        requestChannel.Writer.TryComplete();
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        _cts.Dispose();
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            Logger.LogInfo("Read loop started - waiting for batches from broker");
            var batchesReceived = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var reader = _pipeReader;
                    if (reader == null)
                    {
                        // Nie ma aktualnego połączenia – poczekaj i spróbuj ponownie
                        await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    var result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    var buffer = result.Buffer;

                    if (result.IsCompleted && buffer.Length == 0)
                    {
                        // Broker zamknął połączenie (np. restart) → reconnect
                        Logger.LogWarning("Read loop: stream completed (likely broker restart), attempting reconnect...");
                        await ReconnectAsync().ConfigureAwait(false);
                        continue;
                    }

                    Logger.LogDebug($"Read {buffer.Length} bytes from broker");

                    while (TryReadBatchMessage(ref buffer, out var batchBytes))
                    {
                        batchesReceived++;
                        Logger.LogInfo($"📥 Received batch #{batchesReceived} from broker: {batchBytes.Length} bytes");
                        await responseChannel.Writer.WriteAsync(batchBytes, cancellationToken).ConfigureAwait(false);
                    }

                    reader.AdvanceTo(buffer.Start, buffer.End);
                }
                catch (IOException ex)
                {
                    Logger.LogWarning($"Read loop IO error: {ex.Message}, attempting reconnect...");
                    await ReconnectAsync().ConfigureAwait(false);
                }
                catch (SocketException ex)
                {
                    Logger.LogWarning($"Read loop socket error: {ex.Message}, attempting reconnect...");
                    await ReconnectAsync().ConfigureAwait(false);
                }
                catch (SubscriberConnectionException ex)
                {
                    Logger.LogWarning($"Read loop connection exception: {ex.Message}, attempting reconnect...");
                    await ReconnectAsync().ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Read loop cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Unexpected error in read loop: {ex.Message}");
        }
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in requestChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var writer = _pipeWriter;
                        if (writer == null)
                        {
                            Logger.LogWarning("Write loop: no active writer, attempting reconnect...");
                            await ReconnectAsync().ConfigureAwait(false);
                            continue;
                        }

                        var span = writer.GetSpan(message.Length);
                        message.CopyTo(span);
                        writer.Advance(message.Length);
                        var result = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

                        Logger.LogDebug($"Sent request to broker: {message.Length} bytes");

                        if (result.IsCompleted)
                        {
                            // Stream się zamknął – broker restart lub koniec połączenia
                            Logger.LogWarning("Write loop: flush completed (likely broker restart), attempting reconnect...");
                            await ReconnectAsync().ConfigureAwait(false);
                        }

                        break;
                    }
                    catch (Exception ex) when (ex is IOException || ex is SocketException || ex is InvalidOperationException)
                    {
                        Logger.LogWarning($"Write loop error: {ex.Message}, attempting reconnect...");
                        await ReconnectAsync().ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Write loop cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Unexpected error in write loop: {ex.Message}");
        }
        finally
        {
            var writer = _pipeWriter;
            if (writer != null)
            {
                try { await writer.CompleteAsync().ConfigureAwait(false); }
                catch (Exception ex) { Logger.LogDebug($"Error completing pipe writer: {ex.Message}"); }
            }
        }
    }

    private async Task ReconnectAsync()
    {
        if (Token.IsCancellationRequested)
            return;

        await _reconnectLock.WaitAsync(Token).ConfigureAwait(false);
        try
        {
            if (Token.IsCancellationRequested)
                return;

            Logger.LogInfo("Attempting reconnect...");

            await CompletePipesAsync().ConfigureAwait(false);
            CloseSocketResources();

            await Task.Delay(ReconnectDelayMs, Token).ConfigureAwait(false);
            await EstablishConnectionAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Reconnect cancelled (shutting down).");
        }
        catch (SocketException ex)
        {
            Logger.LogError($"Socket error during reconnect: {ex.Message}");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Unexpected error during reconnect: {ex.Message}");
        }
        finally
        {
            _reconnectLock.Release();
        }
    }

    private async Task CompletePipesAsync()
    {
        var reader = _pipeReader;
        if (reader != null)
        {
            try { await reader.CompleteAsync().ConfigureAwait(false); }
            catch (Exception ex) { Logger.LogDebug($"Error completing pipe reader: {ex.Message}"); }
            _pipeReader = null;
        }

        var writer = _pipeWriter;
        if (writer != null)
        {
            try { await writer.CompleteAsync().ConfigureAwait(false); }
            catch (Exception ex) { Logger.LogDebug($"Error completing pipe writer: {ex.Message}"); }
            _pipeWriter = null;
        }
    }

    private void CloseSocketResources()
    {
        if (_stream != null)
        {
            try { _stream.Dispose(); }
            catch (Exception ex) { Logger.LogDebug($"Error disposing stream: {ex.Message}"); }
            _stream = null;
        }

        if (_client != null)
        {
            try
            {
                if (_client.Connected)
                    _client.Client.Shutdown(SocketShutdown.Both);
            }
            catch (SocketException ex)
            {
                Logger.LogInfo($"Socket already closed while shutting down: {ex.SocketErrorCode}");
            }
            finally
            {
                _client.Close();
                _client = null;
            }
        }
    }

    private bool TryReadBatchMessage(ref ReadOnlySequence<byte> buffer, out byte[] batchBytes)
    {
        batchBytes = Array.Empty<byte>();
        const int minHeaderSize = 24;
        if (buffer.Length < minHeaderSize) return false;

        Span<byte> headerSpan = stackalloc byte[minHeaderSize];
        buffer.Slice(0, minHeaderSize).CopyTo(headerSpan);

        var baseOffset = BinaryPrimitives.ReadUInt64LittleEndian(headerSpan.Slice(0, 8));
        var batchLength = BinaryPrimitives.ReadUInt32LittleEndian(headerSpan.Slice(8, 4));
        var lastOffset = BinaryPrimitives.ReadUInt64LittleEndian(headerSpan.Slice(12, 8));
        var recordBytesLength = BinaryPrimitives.ReadUInt32LittleEndian(headerSpan.Slice(20, 4));

        Logger.LogDebug($"Reading batch header: baseOffset={baseOffset}, batchLength={batchLength}, lastOffset={lastOffset}, recordBytesLength={recordBytesLength}, bufferLength={buffer.Length}");

        if (batchLength == 0)
        {
            buffer = buffer.Slice(minHeaderSize);
            return false;
        }

        const int headerSize = 20;
        var totalBatchSize = headerSize + sizeof(uint) + (int)batchLength;

        if (buffer.Length < totalBatchSize) return false;

        batchBytes = buffer.Slice(0, totalBatchSize).ToArray();
        buffer = buffer.Slice(totalBatchSize);

        Logger.LogInfo($"Received batch: baseOffset={baseOffset}, lastOffset={lastOffset}, batchLength={batchLength} bytes");
        return true;
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
