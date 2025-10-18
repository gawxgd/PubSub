using System.Collections.Generic;
using System.Threading.Tasks;
using SchemaRegistry.Domain.Models;

namespace SchemaRegistry.Infrastructure.Storage
{
    public interface ISchemaStore
    {
        Task<SchemaEntity?> GetByIdAsync(int id);
        Task<SchemaEntity?> GetByChecksumAsync(string checksum);
        Task<SchemaEntity?> GetLatestForTopicAsync(string topic);
        Task<IEnumerable<SchemaEntity>> GetAllForTopicAsync(string topic);
        Task<SchemaEntity> SaveAsync(SchemaEntity entity); // returns saved entity with Id & Version filled
    }
}