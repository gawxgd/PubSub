using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;

namespace MessageBroker.TcpServer;

public class HandleClientConnectionUseCase(Socket socket)
{
    private readonly Channel<ReadOnlyMemory<byte>> _messageChannel =
        Channel.CreateBounded<ReadOnlyMemory<byte>>(new BoundedChannelOptions(100)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

    private readonly Pipe _pipe = new();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var fillTask = FillPipeAsync(cancellationToken);
        var parseTask = ProcessPipeAsync(cancellationToken);
        var consumeTask = ConsumeMessagesAsync(cancellationToken);
        await Task.WhenAll(fillTask, parseTask, consumeTask);

        socket.Dispose();
    }

    private async Task FillPipeAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = _pipe.Writer.GetMemory(4096);
                var bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, cancellationToken);

                if (bytesRead == 0) break; // client disconnected

                _pipe.Writer.Advance(bytesRead);

                var result = await _pipe.Writer.FlushAsync(cancellationToken);
                if (result.IsCompleted) break;
            }
        }
        catch (SocketException)
        {
            /* handle error */
        }
        finally
        {
            await _pipe.Writer.CompleteAsync();
        }
    }

    private async Task ProcessPipeAsync(CancellationToken cancellationToken)
    {
        var reader = _pipe.Reader;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (buffer.Length > 0)
                {
                    var capturedBuffer = buffer.ToArray();
                    await _messageChannel.Writer.WriteAsync(capturedBuffer, cancellationToken);
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted) break;
            }
        }
        finally
        {
            _messageChannel.Writer.Complete();
            await reader.CompleteAsync();
        }
    }

    private async Task ConsumeMessagesAsync(CancellationToken cancellationToken)
    {
        await foreach (var message in _messageChannel.Reader.ReadAllAsync(cancellationToken))
            try
            {
                // Example processing (can be async business logic, parsing, etc.)
                Console.WriteLine($"[{socket.RemoteEndPoint}] Received {message.Length} bytes");

                // Example echo response
                await socket.SendAsync(message, SocketFlags.None, cancellationToken);
            }
            catch (SocketException ex)
            {
                Console.WriteLine($"Socket send error: {ex.Message}");
                break;
            }
    }
}