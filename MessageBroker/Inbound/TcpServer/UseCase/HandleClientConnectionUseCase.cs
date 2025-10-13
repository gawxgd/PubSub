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

    public async Task HandleConnection(CancellationToken cancellationToken)
    {
        try
        {
            var fillTask = FillPipeAsync(cancellationToken);
            var processTask = ProcessPipeAsync(cancellationToken);
            var consumeTask = ConsumeMessageChanelAsync(cancellationToken);

            await Task.WhenAll(fillTask, processTask, consumeTask);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Client handler exception: {ex.Message}");
        }
        finally
        {
            try
            {
                socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Socket shutdown exception", ex);
            }

            socket.Dispose();
            // this socket does not leave this class it supports only reads
            await _pipe.Reader.CompleteAsync();
            await _pipe.Writer.CompleteAsync();
            _messageChannel.Writer.Complete();
        }
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
        catch (Exception ex)
        {
            Console.WriteLine("Exception caught while filling pipe", ex);
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
        catch (Exception ex)
        {
            Console.WriteLine("Exception caught while processing pipe", ex);
        }
        finally
        {
            _messageChannel.Writer.Complete();
            await reader.CompleteAsync();
        }
    }

    private async Task ConsumeMessageChanelAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in _messageChannel.Reader.ReadAllAsync(cancellationToken))
            {
                Console.WriteLine($"[{socket.RemoteEndPoint}] Received {message.Length} bytes");

                await new ProcessReceivedMessageUseCase()
                    .ProcessMessageAsync(message,
                        cancellationToken); // ToDo decide If should be awaited or run on separate thread

                await socket.SendAsync(message, SocketFlags.None,
                    cancellationToken); // for e2e test ToDo delete after sending to subscirber
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception caught while processing message", ex);
        }
    }
}