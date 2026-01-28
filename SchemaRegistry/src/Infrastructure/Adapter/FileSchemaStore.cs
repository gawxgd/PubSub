using System.Text.Json;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Domain.Port;

namespace SchemaRegistry.Infrastructure.Adapter;

public class FileSchemaStore : ISchemaStore
{
    private readonly string _folder;
    private readonly string _indexFile;
    private readonly object _lock = new();
    
    // In-memory index for faster reads (loaded at startup)
    private List<SchemaEntity> _entities = new();
    
    public FileSchemaStore(string folder)
    {
        _folder = folder;
        _indexFile = Path.Combine(_folder, "index.json");
        Directory.CreateDirectory(_folder);
        LoadIndex();
    }

    private void LoadIndex()
    {
        lock (_lock)
        {
            if (!File.Exists(_indexFile))
            {
                _entities = new List<SchemaEntity>();
                SaveIndex();
            }
            else
            {
                var txt = File.ReadAllText(_indexFile);
                _entities = JsonSerializer.Deserialize<List<SchemaEntity>>(txt) ?? new List<SchemaEntity>();
            }
        }
    }
    
    private void SaveIndex()
    {
        lock (_lock)
        {
            var txt = JsonSerializer.Serialize(_entities);
            File.WriteAllText(_indexFile, txt);
        }
    }
    
    public Task<SchemaEntity?> GetByIdAsync(int id)
        => Task.FromResult(_entities.FirstOrDefault(e => e.Id == id));

    public Task<SchemaEntity?> GetByChecksumAsync(string checksum)
        => Task.FromResult(_entities.FirstOrDefault(e => e.Checksum == checksum));

    public Task<SchemaEntity?> GetLatestForTopicAsync(string subject)
        => Task.FromResult(_entities
            .Where(e => e.Topic.Equals(subject, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(e => e.Version)
            .FirstOrDefault());

    public Task<IEnumerable<SchemaEntity>> GetAllForTopicAsync(string subject)
        => Task.FromResult(_entities
            .Where(e => e.Topic.Equals(subject, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(e => e.Version)
            .AsEnumerable());

    public Task<SchemaEntity> SaveAsync(SchemaEntity entity)
    {
        lock (_lock)
        {
            // assign global id
            var nextId = (_entities.Any() ? _entities.Max(e => e.Id) : 0) + 1;

            // compute next version for subject
            var nextVersion = (_entities.Where(e => e.Topic.Equals(entity.Topic, StringComparison.OrdinalIgnoreCase))
                .Any())
                ? _entities.Where(e => e.Topic.Equals(entity.Topic, StringComparison.OrdinalIgnoreCase))
                    .Max(e => e.Version) + 1
                : 1;

            entity.Id = nextId;
            entity.Version = nextVersion;

            _entities.Add(entity);
            SaveIndex();

            // also persist schema body to file for convenience
            var schemaFile = Path.Combine(_folder, $"{entity.Id}.schema.json");
            File.WriteAllText(schemaFile, entity.SchemaJson);

            return Task.FromResult(entity);
        }
    }
}