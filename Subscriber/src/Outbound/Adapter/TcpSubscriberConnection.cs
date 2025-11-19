using System.Buffers;
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
    ChannelWriter<byte[]> messageChannelWriter)
    : ISubscriberConnection, IAsyncDisposable
{
    private readonly TcpClient _client = new();
    private readonly CancellationTokenSource _cancellationSource = new();
    private PipeReader? _pipeReader;
    private PipeWriter? _pipeWriter;
    private Task? _readLoopTask;
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpSubscriberConnection>(LogSource.MessageBroker);

    public async Task ConnectAsync()
    {
        try
        {
            await _client.ConnectAsync(host, port);
            var stream = _client.GetStream();
            _pipeReader = PipeReader.Create(stream);
            _pipeWriter = PipeWriter.Create(stream);
            
            // Send first message with connection type byte (0x02 = Subscriber)
            var connectionTypeMessage = new byte[] { 0x02 };
            var buffer = _pipeWriter.GetMemory(connectionTypeMessage.Length);
            connectionTypeMessage.CopyTo(buffer);
            _pipeWriter.Advance(connectionTypeMessage.Length);
            await _pipeWriter.FlushAsync(_cancellationSource.Token);
            
            _readLoopTask = Task.Run(() => ReadLoopAsync(_cancellationSource.Token), _cancellationSource.Token);
            Logger.LogInfo($"Connected to broker at {_client.Client.RemoteEndPoint} and sent subscriber handshake");
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
            
            if (_pipeWriter != null)
                await _pipeWriter.CompleteAsync();
            
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

                while (TryReadMessage(ref buffer, out var message))
                {
                    await messageChannelWriter.WriteAsync(message, cancellationToken);
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


    private bool TryReadMessage(ref ReadOnlySequence<byte> buffer, out byte[] message)
    {
        var newline = buffer.PositionOf((byte)'\n');
        if (newline == null)
        {
            message = [];
            return false;
        }

        var slice = buffer.Slice(0, newline.Value);
        message = slice.ToArray();

        buffer = buffer.Slice(buffer.GetPosition(1, newline.Value));
        Logger.LogInfo( "Received message");
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
