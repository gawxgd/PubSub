using System.Collections.Generic;
using System.Threading.Tasks;
using SchemaRegistry.Domain.Models;

namespace SchemaRegistry.Domain.Services
{
    public interface ISchemaRegistryService
    {
        Task<int> RegisterSchemaAsync(string topic, string schemaJson);
        Task<SchemaEntity?> GetSchemaByIdAsync(int id);
        Task<SchemaEntity?> GetLatestSchemaAsync(string topic);
        
        Task<IEnumerable<SchemaEntity>> GetVersionsAsync(string topic); // TODO: do we need this?
    }
}