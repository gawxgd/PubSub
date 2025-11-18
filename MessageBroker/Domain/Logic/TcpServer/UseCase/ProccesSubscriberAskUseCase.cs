using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProccesSubscriberAskUseCase(ICommitLogFactory commitLogFactory, string @default)
{
    public async Task ProccesAskAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}