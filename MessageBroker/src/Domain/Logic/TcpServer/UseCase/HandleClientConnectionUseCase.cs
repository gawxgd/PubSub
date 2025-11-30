using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public abstract class HandleClientConnectionUseCase(
    Socket socket,
    Action onConnectionClosed)
{
    protected readonly string ConnectedClientEndpoint = socket.RemoteEndPoint?.ToString() ?? "Unknown";

    protected static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<HandleClientConnectionUseCase>(LogSource.MessageBroker);

    protected readonly Channel<ReadOnlyMemory<byte>> MessageChannel =
        Channel.CreateBounded<ReadOnlyMemory<byte>>(new BoundedChannelOptions(100)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

    protected Socket Socket => socket;

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
            Logger.LogInfo($"Connection cancelled for {ConnectedClientEndpoint}");
        }
        catch (SocketException ex)
        {
            Logger.LogError($"Socket error for {ConnectedClientEndpoint}: {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Client handler exception for {ConnectedClientEndpoint}: {ex.Message}", ex);
        }
        finally
        {
            await CleanupAsync(cancellationToken);
        }
    }

    private async Task FillPipeAsync(CancellationToken cancellationToken)
    {
        Logger.LogInfo($"Filling pipe, connected to client {ConnectedClientEndpoint}");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = _pipe.Writer.GetMemory(4096);
                var bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, cancellationToken);

                if (bytesRead == 0)
                {
                    Logger.LogInfo($"Client {ConnectedClientEndpoint} disconnected");
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
            Logger.LogInfo($"FillPipe completed for {ConnectedClientEndpoint}");
        }
    }

    private async Task ProcessPipeAsync(CancellationToken cancellationToken)
    {
        Logger.LogInfo($"Processing pipe, connected to client {ConnectedClientEndpoint}");
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
                    await MessageChannel.Writer.WriteAsync(capturedBuffer, cancellationToken);
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted)
                {
                    Logger.LogInfo($"ProcessPipe completed for {ConnectedClientEndpoint}");
                    break;
                }
            }
        }
        finally
        {
            MessageChannel.Writer.Complete();
            Logger.LogInfo($"ProcessPipe completed for {ConnectedClientEndpoint}");
        }
    }

    protected abstract Task ConsumeMessageChannelAsync(CancellationToken cancellationToken);
    

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Socket shutdown exception for {ConnectedClientEndpoint}: {ex.Message}", ex);
        }

        socket.Dispose();

        // Ensure pipe is fully completed
        await _pipe.Reader.CompleteAsync();
        await _pipe.Writer.CompleteAsync();

        if (!cancellationToken.IsCancellationRequested)
        {
            Logger.LogInfo($"Calling onConnectionClosed for {ConnectedClientEndpoint}");
            onConnectionClosed();
        }

        Logger.LogInfo($"End of handling connection with client: {ConnectedClientEndpoint}");
    }
}