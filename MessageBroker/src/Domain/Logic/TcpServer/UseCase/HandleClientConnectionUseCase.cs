using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class HandleClientConnectionUseCase(
    Socket socket,
    Action onConnectionClosed,
    IMessageProcessorUseCase messageProcessorUseCase,
    ICommitLogFactory commitLogFactory,
    ILogRecordBatchReader batchReader) : IHandleClientConnectionUseCase
{
    private readonly string _connectedClientEndpoint = socket.RemoteEndPoint?.ToString() ?? "Unknown";

    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<HandleClientConnectionUseCase>(LogSource.MessageBroker);

    private readonly Channel<ReadOnlyMemory<byte>> _messageChannel =
        Channel.CreateBounded<ReadOnlyMemory<byte>>(new BoundedChannelOptions(100)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

    private Socket Socket => socket;

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
            Logger.LogInfo($"Connection cancelled for {_connectedClientEndpoint}");
        }
        catch (SocketException ex)
        {
            Logger.LogError($"Socket error for {_connectedClientEndpoint}: {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Client handler exception for {_connectedClientEndpoint}: {ex.Message}", ex);
        }
        finally
        {
            await CleanupAsync(cancellationToken);
        }
    }

    private async Task FillPipeAsync(CancellationToken cancellationToken)
    {
        Logger.LogInfo($"Filling pipe, connected to client {_connectedClientEndpoint}");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = _pipe.Writer.GetMemory(4096);
                var bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, cancellationToken);

                if (bytesRead == 0)
                {
                    Logger.LogInfo($"Client {_connectedClientEndpoint} disconnected");
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
            Logger.LogInfo($"FillPipe completed for {_connectedClientEndpoint}");
        }
    }

    private async Task ProcessPipeAsync(CancellationToken cancellationToken)
    {
        Logger.LogInfo($"Processing pipe, connected to client {_connectedClientEndpoint}");
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
                    Logger.LogInfo($"ProcessPipe completed for {_connectedClientEndpoint}");
                    break;
                }
            }
        }
        finally
        {
            _messageChannel.Writer.Complete();
            Logger.LogInfo($"ProcessPipe completed for {_connectedClientEndpoint}");
        }
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Socket shutdown exception for {_connectedClientEndpoint}: {ex.Message}", ex);
        }

        socket.Dispose();

        await _pipe.Reader.CompleteAsync();
        await _pipe.Writer.CompleteAsync();

        if (!cancellationToken.IsCancellationRequested)
        {
            Logger.LogInfo($"Calling onConnectionClosed for {_connectedClientEndpoint}");
            onConnectionClosed();
        }

        Logger.LogInfo($"End of handling connection with client: {_connectedClientEndpoint}");
    }

    private async Task ConsumeMessageChannelAsync(CancellationToken cancellationToken)
    {
        Logger.LogInfo(
            $"Consuming message channel, connected to client {_connectedClientEndpoint}");

        await foreach (var message in _messageChannel.Reader.ReadAllAsync(cancellationToken))
        {
            Logger.LogInfo($"[{_connectedClientEndpoint}] Received {message.Length} bytes");

            await messageProcessorUseCase.ProcessAsync(message, Socket, cancellationToken);
        }

        Logger.LogInfo($"ConsumeMessageChannel completed for {_connectedClientEndpoint}");
    }
}