using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib;
using ILogger = LoggerLib.ILogger;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class HandleClientConnectionUseCase(Socket socket, Action onConnectionClosed, ILogger logger)
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
            logger.LogInfo(LogSource.MessageBroker,  $"Connection cancelled for {_connectedClientEndpoint}");
        }
        catch (SocketException ex)
        {
            logger.LogError(LogSource.MessageBroker, $"Socket error for {_connectedClientEndpoint}: {ex.Message}");
        }
        catch (Exception ex)
        {
            logger.LogError(LogSource.MessageBroker, $"Client handler exception for {_connectedClientEndpoint}: {ex.Message}");
        }
        finally
        {
            await CleanupAsync(cancellationToken);
        }
    }

    private async Task FillPipeAsync(CancellationToken cancellationToken)
    {
        logger.LogInfo(LogSource.MessageBroker, $"Filling pipe, connected to client {_connectedClientEndpoint}");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = _pipe.Writer.GetMemory(4096);
                var bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, cancellationToken);

                if (bytesRead == 0)
                {
                    logger.LogInfo(LogSource.MessageBroker, $"Client {_connectedClientEndpoint} disconnected");
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
            logger.LogInfo(LogSource.MessageBroker, $"FillPipe completed for {_connectedClientEndpoint}");
        }
    }

    private async Task ProcessPipeAsync(CancellationToken cancellationToken)
    {
        logger.LogInfo(LogSource.MessageBroker, $"Processing pipe, connected to client {_connectedClientEndpoint}");
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
                    logger.LogInfo(LogSource.MessageBroker, $"ProcessPipe completed for {_connectedClientEndpoint}");
                    break;
                }
            }
        }
        finally
        {
            _messageChannel.Writer.Complete();
            logger.LogInfo(LogSource.MessageBroker, $"ProcessPipe completed for {_connectedClientEndpoint}");
        }
    }

    private async Task ConsumeMessageChannelAsync(CancellationToken cancellationToken)
    {
        logger.LogInfo(LogSource.MessageBroker, $"Consuming message channel, connected to client {_connectedClientEndpoint}");

        await foreach (var message in _messageChannel.Reader.ReadAllAsync(cancellationToken))
        {
            logger.LogInfo(LogSource.MessageBroker, $"[{_connectedClientEndpoint}] Received {message.Length} bytes");

            await new ProcessReceivedMessageUseCase(logger)
                .ProcessMessageAsync(message, cancellationToken);

            await socket.SendAsync(message, SocketFlags.None, cancellationToken);
        }

        logger.LogInfo(LogSource.MessageBroker, $"ConsumeMessageChannel completed for {_connectedClientEndpoint}");
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex)
        {
            logger.LogError(LogSource.MessageBroker, $"Socket shutdown exception for {_connectedClientEndpoint}: {ex.Message}");
        }

        socket.Dispose();

        // Ensure pipe is fully completed
        await _pipe.Reader.CompleteAsync();
        await _pipe.Writer.CompleteAsync();

        if (!cancellationToken.IsCancellationRequested)
        {
            logger.LogInfo(LogSource.MessageBroker, $"Calling onConnectionClosed for {_connectedClientEndpoint}");
            onConnectionClosed();
        }

        logger.LogInfo(LogSource.MessageBroker, $"End of handling connection with client: {_connectedClientEndpoint}");
    }
}