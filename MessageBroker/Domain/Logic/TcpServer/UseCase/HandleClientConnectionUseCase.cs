using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class HandleClientConnectionUseCase(Socket socket, Action onConnectionClosed, ILogger<HandleClientConnectionUseCase> logger, ILogger<ProcessReceivedMessageUseCase> processLogger)
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
            logger.LogInformation("Connection cancelled for {Endpoint}", _connectedClientEndpoint);
        }
        catch (SocketException ex)
        {
            logger.LogError(ex, "Socket error for {Endpoint}: {Message}", _connectedClientEndpoint, ex.Message);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Client handler exception for {Endpoint}: {Message}", _connectedClientEndpoint, ex.Message);
        }
        finally
        {
            await CleanupAsync(cancellationToken);
        }
    }

    private async Task FillPipeAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Filling pipe, connected to client {Endpoint}", _connectedClientEndpoint);
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = _pipe.Writer.GetMemory(4096);
                var bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, cancellationToken);

                if (bytesRead == 0)
                {
                    logger.LogInformation("Client {Endpoint} disconnected", _connectedClientEndpoint);
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
            logger.LogInformation("FillPipe completed for {Endpoint}", _connectedClientEndpoint);
        }
    }

    private async Task ProcessPipeAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Processing pipe, connected to client {Endpoint}", _connectedClientEndpoint);
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
                    logger.LogInformation("ProcessPipe detected completion for {Endpoint}", _connectedClientEndpoint);
                    break;
                }
            }
        }
        finally
        {
            _messageChannel.Writer.Complete();
            logger.LogInformation("ProcessPipe completed for {Endpoint}", _connectedClientEndpoint);
        }
    }

    private async Task ConsumeMessageChannelAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Consuming message channel, connected to client {Endpoint}", _connectedClientEndpoint);

        await foreach (var message in _messageChannel.Reader.ReadAllAsync(cancellationToken))
        {
            logger.LogInformation("[{Endpoint}] Received {Length} bytes", _connectedClientEndpoint, message.Length);

            await new ProcessReceivedMessageUseCase(processLogger)
                .ProcessMessageAsync(message, cancellationToken);

            await socket.SendAsync(message, SocketFlags.None, cancellationToken);
        }

        logger.LogInformation("ConsumeMessageChannel completed for {Endpoint}", _connectedClientEndpoint);
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Socket shutdown exception for {Endpoint}: {Message}", _connectedClientEndpoint, ex.Message);
        }

        socket.Dispose();

        // Ensure pipe is fully completed
        await _pipe.Reader.CompleteAsync();
        await _pipe.Writer.CompleteAsync();

        if (!cancellationToken.IsCancellationRequested)
        {
            logger.LogInformation("Calling onConnectionClosed for {Endpoint}", _connectedClientEndpoint);
            onConnectionClosed();
        }
        logger.LogInformation("End of handling connection with client: {Endpoint}", _connectedClientEndpoint);
    }
}