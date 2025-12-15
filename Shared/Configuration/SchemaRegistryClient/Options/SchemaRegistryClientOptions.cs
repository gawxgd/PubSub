using Shared.Domain.Entities.SchemaRegistryClient;

namespace Shared.Configuration.SchemaRegistryClient.Options;

public sealed record SchemaRegistryClientOptions(
    Uri baseAddress,
    TimeSpan timeout,
    TimeSpan cacheExpiration,
    NotFoundBehavior notFoundBehavior);