namespace Shared.Domain.Entities.SchemaRegistryClient;

public sealed record CachedSchema(SchemaInfo schemaInfo, DateTime cachedAt)
{
    public bool IsExpired(TimeSpan expiration) => DateTime.UtcNow - cachedAt > expiration;
}
