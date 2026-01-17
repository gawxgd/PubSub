using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using SchemaRegistry.Domain.Enums;
using SchemaRegistry.Domain.Exceptions;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Domain.Port;

namespace SchemaRegistry.Infrastructure.Adapter;

public class SchemaRegistryService(
    ISchemaStore store,
    ISchemaCompatibilityService compatibility,
    IOptions<SchemaRegistryOptions> options
) : ISchemaRegistryService
{
    private readonly CompatibilityMode _compatMode = options.Value.CompatibilityMode;

    public async Task<int> RegisterSchemaAsync(string topic, string schemaJson)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(schemaJson);
        
        // check if the schema is even a valid json
        try
        {
            _ = JsonDocument.Parse(schemaJson);
        }
        catch (JsonException ex)
        {
            throw new SchemaValidationException("Schema is not a valid JSON document.", ex);
        }
        
        // check if it's a valid avro schema
        try
        {
            var reader = new Chr.Avro.Representation.JsonSchemaReader();
            _ = reader.Read(schemaJson);
        }
        catch (Exception ex)
        {
            throw new SchemaValidationException("Provided schema is not a valid Avro schema.", ex);
        }
        
        var checksum = ComputeChecksum(schemaJson);

        // Deduplicate within the same topic only
        var latest = await store.GetLatestForTopicAsync(topic);
        if (latest != null && latest.Checksum == checksum)
            return latest.Id;
        if (latest != null &&
            !compatibility.IsCompatible(latest.SchemaJson, schemaJson, _compatMode))
            throw new SchemaCompatibilityException($"New schema is not {_compatMode}-compatible with latest for topic.");

        // save the schema (store will assign id)
        var entity = new SchemaEntity
        {
            Topic = topic,
            SchemaJson = schemaJson,
            Checksum = checksum,
            CreatedAt = DateTime.UtcNow
        };

        var created = await store.SaveAsync(entity);
        return created.Id;
    }

    public Task<SchemaEntity?> GetLatestSchemaAsync(string subject)
    {
        return store.GetLatestForTopicAsync(subject);
    }


    public Task<SchemaEntity?> GetSchemaByIdAsync(int id)
    {
        return store.GetByIdAsync(id);
    }


    public Task<IEnumerable<SchemaEntity>> GetVersionsAsync(string subject)
    {
        return store.GetAllForTopicAsync(subject);
    }
    
    private static string ComputeChecksum(string text)
    {
        using var sha = SHA256.Create();
        var bytes = Encoding.UTF8.GetBytes(text);
        var hash = sha.ComputeHash(bytes);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }
}

