namespace SchemaRegistryClient;

/// <summary>
/// Configuration options for the Schema Registry client
/// </summary>
public sealed class SchemaRegistryClientOptions
{
    /// <summary>
    /// Base URI of the schema registry service
    /// </summary>
    public required Uri BaseAddress { get; init; }
    
    /// <summary>
    /// HTTP request timeout
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(10);
    
    /// <summary>
    /// Cache expiration duration. Set to null for no expiration.
    /// </summary>
    public TimeSpan? CacheExpiration { get; init; } = TimeSpan.FromMinutes(5);
    
    /// <summary>
    /// Behavior when schema is not found
    /// </summary>
    public NotFoundBehavior NotFoundBehavior { get; init; } = NotFoundBehavior.ReturnNull;
}

