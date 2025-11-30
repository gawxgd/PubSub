using System.Net.Sockets;
using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class HandlePublisherClientConnectionUseCase(Socket socket, Action onConnectionClosed, ICommitLogFactory commitLogFactory)
    : HandleClientConnectionUseCase(socket, onConnectionClosed)
{
    protected override async Task ConsumeMessageChannelAsync(CancellationToken cancellationToken)
    {
        Logger.LogInfo(
            $"Consuming message channel, connected to client {ConnectedClientEndpoint}");
        
        await foreach (var message in MessageChannel.Reader.ReadAllAsync(cancellationToken))
        {
            Logger.LogInfo($"[{ConnectedClientEndpoint}] Received {message.Length} bytes");

            try
            {
                await new ProcessReceivedPublisherMessageUseCase(commitLogFactory, "default")
                    .ProcessMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                Logger.LogError($"Consume message channel exception for {ConnectedClientEndpoint}", ex);
            }

            await Socket.SendAsync(message, SocketFlags.None, cancellationToken);
        }

        Logger.LogInfo($"ConsumeMessageChannel completed for {ConnectedClientEndpoint}");
    }
}