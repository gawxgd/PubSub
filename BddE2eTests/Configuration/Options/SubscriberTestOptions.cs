namespace BddE2eTests.Configuration.Options;

public sealed record SubscriberTestOptions(
    string BrokerHost,
    int BrokerPort,
    string Topic,
    int PollIntervalMs,
    uint MaxRetryAttempts,
    string SchemaRegistryHost,
    int SchemaRegistryPort,
    int SchemaRegistryTimeoutSeconds
);
