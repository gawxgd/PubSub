namespace Subscriber.Configuration.Options;

public sealed record SubscriberOptions
{
    // Required parameters
    public required Uri MessageBrokerConnectionUri { get; init; }
    public required Uri SchemaRegistryConnectionUri { get; init; }
    
    // Optional with defaults
    public string Host { get; init; } = "127.0.0.1";
    public int Port { get; init; } = 5000;
    public string? Topic { get; init; }
    public int MinMessageLength { get; init; } = 0;
    public int MaxMessageLength { get; init; } = int.MaxValue;
    public int MaxQueueSize { get; init; } = 65536;
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan SchemaRegistryTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public uint MaxRetryAttempts { get; init; } = 3;
}
