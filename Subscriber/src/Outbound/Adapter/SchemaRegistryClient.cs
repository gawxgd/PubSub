using System.Collections.Concurrent;
using System.Text.Json;
using Subscriber.Domain;
using Subscriber.Domain.Model;
using Subscriber.Inbound.DTOs;


namespace Subscriber.Outbound.Adapter;

public class SchemaRegistryClient : ISchemaRegistryClient
{
    private readonly HttpClient _http;
    private readonly ConcurrentDictionary<int, SchemaInfo> _cacheById = new();
    private readonly ConcurrentDictionary<string, SchemaInfo> _cacheByTopic = new();

    public SchemaRegistryClient(HttpClient http)
    {
        _http = http;
    }

    public async Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId)
    {
        if (_cacheById.TryGetValue(schemaId, out var cached))
            return cached;

        var response = await _http.GetAsync($"schema/id/{schemaId}");
        
        if (!response.IsSuccessStatusCode)
            return null;

        var dto = JsonSerializer.Deserialize<SchemaDto>(
            await response.Content.ReadAsStringAsync(),
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (dto == null)
            return null;

        var schema = new SchemaInfo(dto.Id, dto.SchemaJson.GetRawText(), dto.Version);
        
        _cacheById[schemaId] = schema;
        
        return schema;
    }

    public async Task<SchemaInfo?> GetLatestSchemaByTopicAsync(string topic)
    {
        if (_cacheByTopic.TryGetValue(topic, out var cached))
            return cached;

        var response = await _http.GetAsync($"schema/topic/{topic}");
        
        if (!response.IsSuccessStatusCode)
            return null;

        var dto = JsonSerializer.Deserialize<SchemaDto>(
            await response.Content.ReadAsStringAsync(),
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (dto == null)
            return null;

        var schema = new SchemaInfo(dto.Id, dto.SchemaJson.GetRawText(), dto.Version);
        
        _cacheByTopic[topic] = schema;
        _cacheById[schema.SchemaId] = schema;
        
        return schema;
    }
}