using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using System.Net.Sockets;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class PublisherMessageChannelConsumer(ICommitLogFactory commitLogFactory, string topic) : IConsumeMessageChannelUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<PublisherMessageChannelConsumer>(LogSource.MessageBroker);

    public async Task ConsumeMessageAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        Logger.LogInfo($"Received {message.Length} bytes");

        try
        {
            await new ProcessReceivedPublisherMessageUseCase(commitLogFactory, topic)
                .ProcessMessageAsync(message, cancellationToken);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Consume message channel exception", ex);
        }
    }
}

