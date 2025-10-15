using System.Collections.Concurrent;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Port;

namespace MessageBroker.Inbound.Adapter;

public class InMemoryConnectionRepository : IConnectionRepository
{
    private readonly ConcurrentDictionary<long, Connection> _connections = new();
    private long _lastConnectionId;

    public long GenerateConnectionId()
    {
        return Interlocked.Increment(ref _lastConnectionId);
    }

    public void Add(Connection connection)
    {
        if (!_connections.TryAdd(connection.Id, connection))
            throw new InvalidOperationException($"Connection with id {connection.Id} already exists.");
    }

    public bool Remove(long connectionId)
    {
        return _connections.TryRemove(connectionId, out _);
    }

    public Connection? Get(long connectionId)
    {
        _connections.TryGetValue(connectionId, out var connection);
        return connection;
    }

    public IReadOnlyCollection<Connection> GetAll()
    {
        return _connections.Values.ToList();
    }

    public void RemoveAll()
    {
        _connections.Clear();
    }
}