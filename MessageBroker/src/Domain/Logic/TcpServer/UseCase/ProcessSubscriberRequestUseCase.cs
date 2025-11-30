using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessSubscriberRequestUseCase(Socket socket, ICommitLogFactory commitLogFactory)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessSubscriberRequestUseCase>(LogSource.MessageBroker);
    
    public async Task ProcessRequestAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        // TODO read topic and offset from message
        var topic = "default";
        ulong offset = 0;
        
        Logger.LogDebug($"Processing subscriber request: {message.Length} bytes");
        var commitLogReader = commitLogFactory.GetReader(topic, offset);
        
        var channel = Channel.CreateBounded<byte[]>(10);
        var pipe = new Pipe();
        
        var readTask = Task.Run(async () =>
        {
            try
            {
                await commitLogReader.ReadAsync(channel);
            }
            finally
            {
                channel.Writer.Complete();
            }
        }, cancellationToken);
        
        var writeToPipeTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var messageBytes in channel.Reader.ReadAllAsync(cancellationToken))
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    var writer = pipe.Writer;
                    
                    var payloadSpan = writer.GetSpan(messageBytes.Length);
                    messageBytes.CopyTo(payloadSpan);
                    writer.Advance(messageBytes.Length);
                    
                    await writer.FlushAsync(cancellationToken);
                    Logger.LogDebug($"Written message to pipe: {messageBytes.Length} bytes");
                }
            }
            finally
            {
                await pipe.Writer.CompleteAsync();
            }
        }, cancellationToken);
        
        var sendTask = Task.Run(async () =>
        {
            try
            {
                var reader = pipe.Reader;
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await reader.ReadAsync(cancellationToken);
                    var buffer = result.Buffer;

                    if (buffer.Length > 0)
                    {
                        foreach (var segment in buffer)
                        {
                            await socket.SendAsync(segment, SocketFlags.None, cancellationToken);
                        }
                    }

                    reader.AdvanceTo(buffer.End);

                    if (result.IsCompleted)
                    {
                        break;
                    }
                }
            }
            catch (SocketException ex)
            {
                Logger.LogError($"Socket error while sending to subscriber: {ex.Message}", ex);
            }
            catch (OperationCanceledException)
            {
                Logger.LogInfo("Subscriber connection cancelled");
            }
            finally
            {
                await pipe.Reader.CompleteAsync();
            }
        }, cancellationToken);
        
        try
        {
            await Task.WhenAll(readTask, writeToPipeTask, sendTask);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Error in subscriber processing: {ex.Message}", ex);
        }
        
        Logger.LogInfo("Messages from commit log sent to subscriber");
    }
}