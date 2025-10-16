namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedMessageUseCase
{
    public async Task ProcessMessageAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Processing message {message.Length} bytes");
    }
}