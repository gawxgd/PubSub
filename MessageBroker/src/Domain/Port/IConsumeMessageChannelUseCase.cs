namespace MessageBroker.Domain.Port;

public interface IConsumeMessageChannelUseCase
{
    Task ConsumeMessageAsync(ReadOnlyMemory<byte> message, System.Net.Sockets.Socket socket, CancellationToken cancellationToken);
}

