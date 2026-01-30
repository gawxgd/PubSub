namespace PubSubDemo.Configuration;

public sealed class BrokerOptions
{
    public string Host { get; set; } = "127.0.0.1";

    public int Port { get; set; } = 9096;

    public int SubscriberPort { get; set; } = 9098;

    public uint MaxQueueSize { get; set; } = 10000;
}

