using System.Security.Cryptography;
using System.Text;
using SchemaRegistry.Domain.Enums;
using SchemaRegistry.Domain.Exceptions;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Domain.Port;
using SchemaRegistry.Infrastructure.Storage;

namespace SchemaRegistry.Outbound.Adapter;

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
        
        var checksum = ComputeChecksum(schemaJson);

        // If same schema exists globally -> return its id (dedupe)
        var existing = await _store.GetByChecksumAsync(checksum);
        if (existing != null)
            return existing.Id;

        // get latest for topic to check compatibility
        var latest = await _store.GetLatestForTopicAsync(topic);

        if (latest != null && !_compatibility.IsCompatible(latest.SchemaJson, schemaJson, _compatMode))
            throw new SchemaCompatibilityException("New schema is not compatible with latest for topic.");

        // TODO: maybe check if the schema json is valid if we are to create a new one?

        // create new id (store will assign)
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

