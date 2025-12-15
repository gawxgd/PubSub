namespace Subscriber.Configuration.Options;

public class SubscriberOptions
{
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 5000;
    public string? Topic { get; set; }
    public int MinMessageLength { get; set; }
    public int MaxMessageLength { get; set; }
    public TimeSpan PollInterval { get; set; }
    public uint MaxRetryAttempts { get; set; }

    public required Uri MessageBrokerConnectionUri;
    
    public required Uri SchemaRegistryConnectionUri;
    
    public int MaxQueueSIze { get; set; }
    public int MaxQueueSize { get; } = 65536; // Analize the value here
    
    public TimeSpan SchemaRegistryTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
