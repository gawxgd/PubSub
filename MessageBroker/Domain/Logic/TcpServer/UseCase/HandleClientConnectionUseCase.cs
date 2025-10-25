using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class HandleClientConnectionUseCase(Socket socket, Action onConnectionClosed)
{
    private readonly string _connectedClientEndpoint = socket.RemoteEndPoint?.ToString() ?? "Unknown";

    private readonly IAutoLogger _logger =
        AutoLoggerFactory.CreateLogger<HandleClientConnectionUseCase>(LogSource.MessageBroker);

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
            _logger.LogInfo( $"Connection cancelled for {_connectedClientEndpoint}");
        }
        catch (SocketException ex)
        {
            _logger.LogError($"Socket error for {_connectedClientEndpoint}: {ex.Message}");
        }
        catch (Exception ex)
        {
            _logger.LogError(
                $"Client handler exception for {_connectedClientEndpoint}: {ex.Message}");
        }
        finally
        {
            await CleanupAsync(cancellationToken);
        }
    }

    private async Task FillPipeAsync(CancellationToken cancellationToken)
    {
        _logger.LogInfo($"Filling pipe, connected to client {_connectedClientEndpoint}");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = _pipe.Writer.GetMemory(4096);
                var bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, cancellationToken);

                if (bytesRead == 0)
                {
                    _logger.LogInfo($"Client {_connectedClientEndpoint} disconnected");
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
            _logger.LogInfo($"FillPipe completed for {_connectedClientEndpoint}");
        }
    }

    private async Task ProcessPipeAsync(CancellationToken cancellationToken)
    {
        _logger.LogInfo($"Processing pipe, connected to client {_connectedClientEndpoint}");
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
                    _logger.LogInfo($"ProcessPipe completed for {_connectedClientEndpoint}");
                    break;
                }
            }
        }
        finally
        {
            _messageChannel.Writer.Complete();
            _logger.LogInfo($"ProcessPipe completed for {_connectedClientEndpoint}");
        }
    }

    private async Task ConsumeMessageChannelAsync(CancellationToken cancellationToken)
    {
        _logger.LogInfo(
            $"Consuming message channel, connected to client {_connectedClientEndpoint}");

        await foreach (var message in _messageChannel.Reader.ReadAllAsync(cancellationToken))
        {
            _logger.LogInfo( $"[{_connectedClientEndpoint}] Received {message.Length} bytes");

            await new ProcessReceivedMessageUseCase(_logger)
                .ProcessMessageAsync(message, cancellationToken);

            await socket.SendAsync(message, SocketFlags.None, cancellationToken);
        }

        _logger.LogInfo($"ConsumeMessageChannel completed for {_connectedClientEndpoint}");
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                $"Socket shutdown exception for {_connectedClientEndpoint}: {ex.Message}");
        }

        socket.Dispose();

        // Ensure pipe is fully completed
        await _pipe.Reader.CompleteAsync();
        await _pipe.Writer.CompleteAsync();

        if (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogInfo($"Calling onConnectionClosed for {_connectedClientEndpoint}");
            onConnectionClosed();
        }

        _logger.LogInfo($"End of handling connection with client: {_connectedClientEndpoint}");
    }
}