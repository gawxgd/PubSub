namespace SchemaRegistryClient;

public sealed class CachedSchema
{
    public SchemaInfo Schema { get; }
    public DateTime CachedAt { get; }

    public CachedSchema(SchemaInfo schema)
    {
        Schema = schema;
        CachedAt = DateTime.UtcNow;
    }

    public bool IsExpired(TimeSpan expiration)
    {
        return DateTime.UtcNow - CachedAt > expiration;
    }
}