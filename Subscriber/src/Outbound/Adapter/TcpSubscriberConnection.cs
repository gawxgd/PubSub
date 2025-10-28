using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using Subscriber.Domain;
using Subscriber.Outbound.Exceptions;

namespace Subscriber.Outbound.Adapter;

public sealed class TcpSubscriberConnection(
    string host,
    int port,
    ChannelWriter<byte[]> messageChannelWriter,
    ILogger logger)
    : ISubscriberConnection, IAsyncDisposable
{
    private readonly TcpClient _client = new();
    private readonly CancellationTokenSource _cancellationSource = new();
    private PipeReader? _pipeReader;
    private Task? _readLoopTask;

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.ConnectAsync(host, port, cancellationToken);
            _pipeReader = PipeReader.Create(_client.GetStream());
            _readLoopTask = Task.Run(() => ReadLoopAsync(_cancellationSource.Token), _cancellationSource.Token);
            logger.LogInfo(LogSource.Subscriber,  $"Connected to broker at {_client.Client.RemoteEndPoint}");
        }
        catch (SocketException ex)
        {
            logger.LogError(LogSource.Subscriber, $"Unexpected error during connection: {ex.Message}");
            throw new SubscriberConnectionException("TCP connection failed", ex);
        }
    }

    public async Task DisconnectAsync()
    {
        try
        {
            await _cancellationSource.CancelAsync();

            if (_readLoopTask != null)
                await _readLoopTask;

            if (_pipeReader != null)
                await _pipeReader.CompleteAsync();
            
            try
            {
                _client.Client.Shutdown(SocketShutdown.Both);
            }
            catch (SocketException ex)
            {
                logger.LogInfo(
                    LogSource.Subscriber,
                    $"Socket already closed or disconnected while shutting down connection : {ex.SocketErrorCode}"
                );
            }

            finally
            {
                _client.Close(); 
            }
            logger.LogInfo(LogSource.Subscriber, $"Disconnected from broker at {_client.Client.RemoteEndPoint}");
        }
        catch (Exception ex)
        {
            logger.LogError(LogSource.Subscriber, $"Error during disconnect: {ex.Message}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
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
                logger.LogInfo(LogSource.Subscriber, $"Disconnected from broker at {_client.Client.RemoteEndPoint}");
                break;
            }
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
        logger.LogInfo(LogSource.Subscriber, "Received message");
        return true;
    }
}
