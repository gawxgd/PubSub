using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using Subscriber.Domain;

namespace Subscriber.Inbound.Adapter;

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

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.ConnectAsync(host, port, cancellationToken);
            _pipeReader = PipeReader.Create(_client.GetStream());
            _readLoopTask = Task.Run(() => ReadLoopAsync(_cancellationSource.Token), _cancellationSource.Token);
            Console.WriteLine($"[Subscriber] Connected to broker at {_client.Client.RemoteEndPoint}");
        }
        catch (SocketException ex)
        {
            throw new SubscriberConnectionException("TCP connection failed", ex);
        }
    }

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _cancellationSource.CancelAsync();

            if (_readLoopTask != null)
                await _readLoopTask;

            if (_pipeReader != null)
                await _pipeReader.CompleteAsync();

            _client.Client.Shutdown(SocketShutdown.Both);
            _client.Close();
            _client.Dispose();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Subscriber] Disconnect error: {ex.Message}");
        }
    }

    public async ValueTask DisposeAsync() => await DisconnectAsync();

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
                break;
        }
    }

    private static bool TryReadMessage(ref ReadOnlySequence<byte> buffer, out byte[] message)
    {
        var newline = buffer.PositionOf((byte)'\n');
        if (newline == null)
        {
            message = Array.Empty<byte>();
            return false;
        }

        var slice = buffer.Slice(0, newline.Value);
        message = slice.ToArray();

        buffer = buffer.Slice(buffer.GetPosition(1, newline.Value));
        return true;
    }
}
