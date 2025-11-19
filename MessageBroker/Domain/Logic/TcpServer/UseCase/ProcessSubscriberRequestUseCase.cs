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
        var topic = string.Empty;
        ulong offset = 0;
        
        
        Logger.LogDebug($"Processing subscriber request: {message}");
        var commitLogReader = commitLogFactory.GetReader(topic, offset);
        //TODO channel size config
        var channel = Channel.CreateBounded<byte[]>(10);
        
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
        
        try
        {
            await foreach (var messageBytes in channel.Reader.ReadAllAsync(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                await socket.SendAsync(messageBytes, SocketFlags.None, cancellationToken);
                
                Logger.LogDebug($"Sent message to subscriber: {messageBytes.Length} bytes");
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
            try
            {
                await readTask;
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error in commit log reader: {ex.Message}", ex);
            }
        }
        Logger.LogInfo("Messages from commit log sent to subscriber");
    }
}