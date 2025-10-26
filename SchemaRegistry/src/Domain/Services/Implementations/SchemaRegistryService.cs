using System.Security.Cryptography;
using System.Text;
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Infrastructure.Storage;
using SchemaRegistry.Infrastructure.Validation;

namespace SchemaRegistry.Domain.Services.Implementations;

public class SchemaRegistryService : ISchemaRegistryService
{
    private readonly ICompatibilityChecker _checker;
    private readonly string _compatMode; // "BACKWARD", "FORWARD", "FULL", "NONE"
    private readonly ISchemaStore _store;

    public SchemaRegistryService(ISchemaStore store, ICompatibilityChecker checker, IConfiguration cfg)
    {
        _store = store;
        _checker = checker;
        _compatMode = cfg.GetValue<string>("SchemaRegistry:CompatibilityMode") ?? "BACKWARD";
    }

    public async Task<int> RegisterSchemaAsync(string topic, string schemaJson)
    {
        if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("topic");
        if (string.IsNullOrWhiteSpace(schemaJson)) throw new ArgumentException("schemaJson");

        var checksum = ComputeChecksum(schemaJson);

        // If same schema exists globally -> return its id (dedupe)
        var existing = await _store.GetByChecksumAsync(checksum);
        if (existing != null)
            return existing.Id;

        // get latest for topic to check compatibility
        var latest = await _store.GetLatestForTopicAsync(topic);

        if (latest != null && !IsCompatible(latest.SchemaJson, schemaJson))
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

    /// <summary>
    ///     Interpret compatibility of two Avro schemas based on configuration in _compatMode
    /// </summary>
    private bool IsCompatible(string oldSchemaJson, string newSchemaJson)
    {
        if (_compatMode.Equals("NONE", StringComparison.OrdinalIgnoreCase)) return true;

        var jsonReader = new JsonSchemaReader();

        // parse a schema string (Avro JSON)
        var newSchema = jsonReader.Read(newSchemaJson);
        var oldSchema = jsonReader.Read(oldSchemaJson);

        if (newSchema is RecordSchema newRecordSchema && oldSchema is RecordSchema oldRecordSchema)
        {
            if (_compatMode.Equals("BACKWARD", StringComparison.OrdinalIgnoreCase))
                return _checker.IsBackwardCompatible(newRecordSchema, oldRecordSchema);
            if (_compatMode.Equals("FORWARD", StringComparison.OrdinalIgnoreCase))
                return _checker.IsForwardCompatible(newRecordSchema, oldRecordSchema);
            if (_compatMode.Equals("FULL", StringComparison.OrdinalIgnoreCase))
                return _checker.IsBackwardCompatible(newRecordSchema, oldRecordSchema)
                       && _checker.IsForwardCompatible(newRecordSchema, oldRecordSchema);
        }

        return false;
    }
}

public class SchemaCompatibilityException : Exception
{
    public SchemaCompatibilityException(string message) : base(message)
    {
    }
}