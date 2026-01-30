using SchemaRegistry.Domain.Models;

namespace SchemaRegistry.Domain.Port
{
    public interface ISchemaStore
    {
        Task<SchemaEntity?> GetByIdAsync(int id);
        Task<SchemaEntity?> GetByChecksumAsync(string checksum);
        Task<SchemaEntity?> GetLatestForTopicAsync(string topic);
        Task<IEnumerable<SchemaEntity>> GetAllForTopicAsync(string topic);
        Task<SchemaEntity> SaveAsync(SchemaEntity entity);
    }
}
