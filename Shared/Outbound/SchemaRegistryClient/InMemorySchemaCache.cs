using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Shared.Outbound.SchemaRegistryClient;

public sealed class InMemorySchemaCache(TimeSpan? expiration = null) : ISchemaCache
{
    private readonly ConcurrentDictionary<int, CachedSchema> _byId = new();
    private readonly ConcurrentDictionary<string, CachedSchema> _byTopic = new();

    public bool TryGet(int schemaId, out SchemaInfo schema)
    {
        return TryGetFromCache(_byId, schemaId, out schema);
    }

    public bool TryGet(string topic, out SchemaInfo schema)
    {
        return TryGetFromCache(_byTopic, topic, out schema);
    }

    public void AddToCache(int schemaId, SchemaInfo schema)
    {
        _byId[schemaId] = new CachedSchema(schema, DateTime.UtcNow);
    }

    public void AddToCache(string topic, SchemaInfo schema)
    {
        var cached = new CachedSchema(schema, DateTime.UtcNow);
        _byTopic[topic] = cached;
        _byId[schema.SchemaId] = cached;
    }

    private bool TryGetFromCache<TKey>(ConcurrentDictionary<TKey, CachedSchema> cache, TKey key, out SchemaInfo? schema)
        where TKey : notnull
    {
        if (cache.TryGetValue(key, out var cached) && !IsExpired(cached))
        {
            schema = cached.schemaInfo;
            return true;
        }

        Invalidate(key);
        schema = null;
        return false;
    }

    private bool IsExpired(CachedSchema cached)
    {
        return expiration.HasValue && cached.IsExpired(expiration.Value);
    }

    private void Invalidate<TKey>(TKey key)
    {
        switch (key)
        {
            case int schemaId:
                _byId.TryRemove(schemaId, out _);
                break;
            case string topic when _byTopic.TryRemove(topic, out var removed):
                _byId.TryRemove(removed.schemaInfo.SchemaId, out _);
                break;
        }
    }
}
