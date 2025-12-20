namespace Publisher.Configuration.Options;

public sealed record PublisherOptions(
    Uri MessageBrokerConnectionUri,
    Uri SchemaRegistryConnectionUri,
    TimeSpan SchemaRegistryTimeout,
    string Topic,
    uint MaxPublisherQueueSize,
    uint MaxSendAttempts,
    uint MaxRetryAttempts,
    int BatchMaxBytes,
    TimeSpan BatchMaxDelay);