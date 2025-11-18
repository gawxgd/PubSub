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
        // read topic and offset from message
        var topic = string.Empty;
        ulong offset = 0;
        
        // create channel (pipe?) 
        Channel<byte[]> ? channel = null;
        
        Logger.LogDebug($"Processing subscriber request: {message}");
        var commitLogReader = commitLogFactory.GetReader(topic, offset);
        await commitLogReader.ReadAsync(channel);
        Logger.LogInfo("Messages from commit log sent to subscriber");
    }
}