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
    uint MaxRetryAttempts)
{
    public SubscriberOptions(
        Uri messageBrokerConnectionUri,
        Uri schemaRegistryConnectionUri,
        string? topic = null)
        : this(
            messageBrokerConnectionUri,
            schemaRegistryConnectionUri,
            Host: "127.0.0.1",
            Port: 5000,
            topic,
            MinMessageLength: 0,
            MaxMessageLength: int.MaxValue,
            MaxQueueSize: 65536,
            PollInterval: TimeSpan.FromSeconds(1),
            SchemaRegistryTimeout: TimeSpan.FromSeconds(10),
            MaxRetryAttempts: 3)
    { }
}