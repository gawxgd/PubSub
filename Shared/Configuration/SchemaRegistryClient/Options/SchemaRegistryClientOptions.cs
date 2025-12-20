namespace Shared.Configuration.SchemaRegistryClient.Options;

public sealed record SchemaRegistryClientOptions(
    Uri BaseAddress,
    TimeSpan Timeout,
    TimeSpan? CacheExpiration = null);
