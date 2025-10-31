using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using SchemaRegistry.Domain.Enums;
using SchemaRegistry.Domain.Exceptions;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Domain.Port;
using JsonException = Newtonsoft.Json.JsonException;

namespace SchemaRegistry.Infrastructure.Adapter;

public class SchemaRegistryService : ISchemaRegistryService
{
    private readonly ISchemaCompatibilityService _compatibility;
    private readonly CompatibilityMode _compatMode;
    private readonly ISchemaStore _store;

    public SchemaRegistryService(ISchemaStore store, ISchemaCompatibilityService compatibility, IConfiguration cfg)
    {
        _store = store;
        _compatibility = compatibility;
        var modeString = cfg.GetValue<string>("SchemaRegistry:CompatibilityMode") ?? "FULL";
        _compatMode = Enum.TryParse<CompatibilityMode>(modeString, true, out var mode)
            ? mode
            : CompatibilityMode.Full;
    }

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

        // If same schema exists globally -> return its id (deduplication)
        var existing = await _store.GetByChecksumAsync(checksum);
        if (existing != null)
            return existing.Id;

        // get latest for topic to check compatibility
        var latest = await _store.GetLatestForTopicAsync(topic);
        if (latest != null &&
            !_compatibility.IsCompatible(latest.SchemaJson, schemaJson, _compatMode))
            throw new SchemaCompatibilityException($"New schema is not {_compatMode}-compatible with latest for topic.");

        // save the schema (store will assign id)
        var entity = new SchemaEntity
        {
            Topic = topic,
            SchemaJson = schemaJson,
            Checksum = checksum,
            CreatedAt = DateTime.UtcNow
        };

        var created = await _store.SaveAsync(entity);
        return created.Id;
    }

    public Task<SchemaEntity?> GetLatestSchemaAsync(string subject)
    {
        return _store.GetLatestForTopicAsync(subject);
    }


    public Task<SchemaEntity?> GetSchemaByIdAsync(int id)
    {
        return _store.GetByIdAsync(id);
    }


    public Task<IEnumerable<SchemaEntity>> GetVersionsAsync(string subject)
    {
        return _store.GetAllForTopicAsync(subject);
    }


    private static string ComputeChecksum(string text)
    {
        using var sha = SHA256.Create();
        var bytes = Encoding.UTF8.GetBytes(text);
        var hash = sha.ComputeHash(bytes);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }
}

