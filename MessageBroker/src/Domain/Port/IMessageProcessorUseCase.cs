using System.Net.Sockets;

namespace MessageBroker.Domain.Port;

public interface IMessageProcessorUseCase
{
    Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken);
}

