using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class HandleClientConnectionUseCase(Socket socket, Action onConnectionClosed)
{
    private readonly string _connectedClientEndpoint = socket.RemoteEndPoint?.ToString() ?? "Unknown";

    private readonly Channel<ReadOnlyMemory<byte>> _messageChannel =
        Channel.CreateBounded<ReadOnlyMemory<byte>>(new BoundedChannelOptions(100)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

    private readonly Pipe _pipe = new();

    public async Task HandleConnection(CancellationToken cancellationToken)
    {
        try
        {
            var fillTask = FillPipeAsync(cancellationToken);
            var processTask = ProcessPipeAsync(cancellationToken);
            var consumeTask = ConsumeMessageChannelAsync(cancellationToken);

            await Task.WhenAll(fillTask, processTask, consumeTask);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"Connection cancelled for {_connectedClientEndpoint}");
        }
        catch (SocketException ex)
        {
            Console.WriteLine($"Socket error for {_connectedClientEndpoint}: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Client handler exception for {_connectedClientEndpoint}: {ex.Message}");
        }
        finally
        {
            await CleanupAsync(cancellationToken);
        }
    }

    private async Task FillPipeAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Filling pipe, connected to client {_connectedClientEndpoint}");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = _pipe.Writer.GetMemory(4096);
                var bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, cancellationToken);

                if (bytesRead == 0)
                {
                    Console.WriteLine($"Client {_connectedClientEndpoint} disconnected");
                    break;
                }

                _pipe.Writer.Advance(bytesRead);

                var result = await _pipe.Writer.FlushAsync(cancellationToken);
                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        finally
        {
            await _pipe.Writer.CompleteAsync();
            Console.WriteLine($"FillPipe completed for {_connectedClientEndpoint}");
        }
    }

    private async Task ProcessPipeAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Processing pipe, connected to client {_connectedClientEndpoint}");
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

                if (result.IsCompleted)
                {
                    Console.WriteLine($"ProcessPipe detected completion for {_connectedClientEndpoint}");
                    break;
                }
            }
        }
        finally
        {
            _messageChannel.Writer.Complete();
            Console.WriteLine($"ProcessPipe completed for {_connectedClientEndpoint}");
        }
    }

    private async Task ConsumeMessageChannelAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Consuming message channel, connected to client {_connectedClientEndpoint}");

        await foreach (var message in _messageChannel.Reader.ReadAllAsync(cancellationToken))
        {
            Console.WriteLine($"[{_connectedClientEndpoint}] Received {message.Length} bytes");

            await new ProcessReceivedMessageUseCase()
                .ProcessMessageAsync(message, cancellationToken);

            await socket.SendAsync(message, SocketFlags.None, cancellationToken);
        }

        Console.WriteLine($"ConsumeMessageChannel completed for {_connectedClientEndpoint}");
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Socket shutdown exception for {_connectedClientEndpoint}: {ex.Message}");
        }

        socket.Dispose();

        // Ensure pipe is fully completed
        await _pipe.Reader.CompleteAsync();
        await _pipe.Writer.CompleteAsync();

        if (!cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine($"Calling onConnectionClosed for {_connectedClientEndpoint}");
            onConnectionClosed();
        }

        Console.WriteLine($"End of handling connection with client: {_connectedClientEndpoint}");
    }
}