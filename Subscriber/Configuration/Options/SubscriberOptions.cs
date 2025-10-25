namespace Subscriber.Configuration;

public class SubscriberOptions
{
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 5000;
    public string? Topic { get; set; }
    public int MinMessageLength { get; set; }
    public int MaxMessageLength { get; set; }
    public TimeSpan PollInterval { get; set; }
    public uint MaxRetryAttempts { get; set; }

    public Uri MessageBrokerConnectionUri;
}
