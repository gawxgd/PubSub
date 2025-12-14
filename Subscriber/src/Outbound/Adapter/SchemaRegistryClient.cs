using System.Collections.Concurrent;
using System.Text.Json;
using Subscriber.Domain;
using Subscriber.Domain.Model;
using Subscriber.Inbound.DTOs;

namespace Subscriber.Outbound.Adapter;

public class SchemaRegistryClient : ISchemaRegistryClient
{
    private readonly HttpClient _http;
    private readonly TimeSpan _cacheExpiration;
    private readonly ConcurrentDictionary<int, CachedSchema> _cacheById = new();
    private readonly ConcurrentDictionary<string, CachedSchema> _cacheByTopic = new();

    public SchemaRegistryClient(HttpClient http, TimeSpan? cacheExpiration = null)
    {
        _http = http;
        // Default to 5 minutes if not specified
        _cacheExpiration = cacheExpiration ?? TimeSpan.FromMinutes(5);
    }

    public async Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId)
    {
        if (_cacheById.TryGetValue(schemaId, out var cached))
        {
            if (cached.IsExpired(_cacheExpiration))
            {
                // Remove expired entry
                _cacheById.TryRemove(schemaId, out _);
            }
            else
            {
                return cached.Schema;
            }
        }

        var response = await _http.GetAsync($"schema/id/{schemaId}");
        
        if (!response.IsSuccessStatusCode)
            return null;

        var dto = JsonSerializer.Deserialize<SchemaDto>(
            await response.Content.ReadAsStringAsync(),
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (dto == null)
            return null;

        var schema = new SchemaInfo(dto.Id, dto.SchemaJson.GetRawText(), dto.Version);
        
        _cacheById[schemaId] = new CachedSchema(schema);
        
        return schema;
    }

    public async Task<SchemaInfo?> GetLatestSchemaByTopicAsync(string topic)
    {
        if (_cacheByTopic.TryGetValue(topic, out var cached))
        {
            if (cached.IsExpired(_cacheExpiration))
            {
                // Remove expired entry from both caches
                _cacheByTopic.TryRemove(topic, out _);
                _cacheById.TryRemove(cached.Schema.SchemaId, out _);
            }
            else
            {
                return cached.Schema;
            }
        }

        var response = await _http.GetAsync($"schema/topic/{topic}");
        
        if (!response.IsSuccessStatusCode)
            return null;

        var dto = JsonSerializer.Deserialize<SchemaDto>(
            await response.Content.ReadAsStringAsync(),
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (dto == null)
            return null;

        var schema = new SchemaInfo(dto.Id, dto.SchemaJson.GetRawText(), dto.Version);
        var cachedSchema = new CachedSchema(schema);
        
        _cacheByTopic[topic] = cachedSchema;
        _cacheById[schema.SchemaId] = cachedSchema;
        
        return schema;
    }

    private sealed class CachedSchema
    {
        public SchemaInfo Schema { get; }
        public DateTime CachedAt { get; }

        public CachedSchema(SchemaInfo schema)
        {
            Schema = schema;
            CachedAt = DateTime.UtcNow;
        }

        public bool IsExpired(TimeSpan expiration)
        {
            return DateTime.UtcNow - CachedAt > expiration;
        }
    }
}