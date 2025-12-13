using System.Collections.Concurrent;
using System.Text.Json;
using Publisher.Domain.Model;
using Publisher.Domain.Port;
using Publisher.Inbound.DTOs;

namespace Publisher.Domain.Service;

public class SchemaRegistryClient(HttpClient http) : ISchemaRegistryClient
{
    private readonly ConcurrentDictionary<string, SchemaInfo> _cache = new();

    public async Task<SchemaInfo> GetSchemaAsync(string topic)
    {
        // check if we haven't got the schema cached already - return it if so
        if (_cache.TryGetValue(topic, out var cached))
            return cached;

        // fetch the avro schema for the topic from the SchemaRegistry
        var response = await http.GetAsync($"schema/topic/{topic}"); // TODO adjust path
        response.EnsureSuccessStatusCode();
        
        //process the schema so that it can be read properly
        var dto = JsonSerializer.Deserialize<SchemaDto>(
            await response.Content.ReadAsStringAsync(),
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (dto == null)
            throw new InvalidOperationException("SchemaRegistry returned null schema.");
        
        // Map DTO → Domain model
        var schema = new SchemaInfo(
            id: dto.Id,
            json: dto.SchemaJson.GetRawText(),
            version: dto.Version
        );
        
        // cache it
        _cache[topic] = schema;
        
        // return the schema
        return schema;
    }
}