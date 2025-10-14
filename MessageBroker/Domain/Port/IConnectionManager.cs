using System.Net.Sockets;

namespace MessageBroker.Domain.Port.Repositories;

public interface IConnectionManager
{
    void RegisterConnection(Socket acceptedSocket, CancellationTokenSource cancellationTokenSource);

    Task UnregisterConnectionAsync(long connectionId);

    Task UnregisterAllConnectionsAsync();
}