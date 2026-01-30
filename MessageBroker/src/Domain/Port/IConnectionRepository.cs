using MessageBroker.Domain.Entities;

namespace MessageBroker.Domain.Port;

public interface IConnectionRepository
{
    long GenerateConnectionId();
    void Add(Connection connection);
    bool Remove(long connectionId);
    Connection? Get(long connectionId);
    IReadOnlyCollection<Connection> GetAll();
    void RemoveAll();
}
