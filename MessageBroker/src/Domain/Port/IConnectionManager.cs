using System.Net.Sockets;
using MessageBroker.Domain.Enums;

namespace MessageBroker.Domain.Port;

public interface IConnectionManager
{
    void RegisterConnection(ConnectionType connectionType, Socket acceptedSocket, CancellationTokenSource cancellationTokenSource);
    Task UnregisterConnectionAsync(long connectionId);
    Task UnregisterAllConnectionsAsync();
}
