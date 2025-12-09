using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using System.Net.Sockets;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class SubscriberMessageChannelConsumer(ICommitLogFactory commitLogFactory) : IConsumeMessageChannelUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<SubscriberMessageChannelConsumer>(LogSource.MessageBroker);

    public async Task ConsumeMessageAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        Logger.LogInfo($"Received {message.Length} bytes");

        try
        {
            await new ProcessSubscriberRequestUseCase(socket, commitLogFactory)
                .ProcessRequestAsync(message, cancellationToken);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Consume message channel exception", ex);
        }
    }
}

