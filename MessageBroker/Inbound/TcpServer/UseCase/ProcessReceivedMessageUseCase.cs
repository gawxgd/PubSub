namespace MessageBroker.TcpServer;

public class ProcessReceivedMessageUseCase
{
    public async Task ProcessMessageAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
    }
}