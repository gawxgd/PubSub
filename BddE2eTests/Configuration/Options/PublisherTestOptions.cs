namespace BddE2eTests.Configuration.Options;

public sealed record PublisherTestOptions(
    string BrokerHost,
    int BrokerPort,
    string Topic,
    uint MaxPublisherQueueSize,
    uint MaxSendAttempts,
    uint MaxRetryAttempts,
    string SchemaRegistryHost,
    int SchemaRegistryPort,
    int BatchMaxBytes,
    int BatchMaxDelayMs,
    int SchemaRegistryTimeoutSeconds
);
