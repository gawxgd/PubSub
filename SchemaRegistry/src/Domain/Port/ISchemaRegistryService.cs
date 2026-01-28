using SchemaRegistry.Domain.Models;

namespace SchemaRegistry.Domain.Port
{
    public interface ISchemaRegistryService
    {
        Task<int> RegisterSchemaAsync(string topic, string schemaJson);
        Task<SchemaEntity?> GetSchemaByIdAsync(int id);
        Task<SchemaEntity?> GetLatestSchemaAsync(string topic);
        
        Task<IEnumerable<SchemaEntity>> GetVersionsAsync(string topic);
    }
}