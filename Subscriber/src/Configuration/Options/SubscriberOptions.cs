namespace Subscriber.Configuration.Options;

public sealed record SubscriberOptions(
    Uri MessageBrokerConnectionUri,
    Uri SchemaRegistryConnectionUri,
    string Host,
    int Port,
    string? Topic,
    int MinMessageLength,
    int MaxMessageLength,
    int MaxQueueSize,
    TimeSpan PollInterval,
    TimeSpan SchemaRegistryTimeout,
    uint MaxRetryAttempts
);
