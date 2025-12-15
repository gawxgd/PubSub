namespace Shared.Domain.Entities;

public sealed record CachedSchema(SchemaInfo schemaInfo, DateTime cachedAt)
{
    public bool IsExpired(TimeSpan expiration) => DateTime.UtcNow - cachedAt > expiration;
}