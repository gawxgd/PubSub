namespace PubSubDemo.Configuration;

/// <summary>
/// Configuration options for connecting to the message broker.
/// </summary>
public sealed class BrokerOptions
{
    /// <summary>
    /// Hostname or IP address of the broker.
    /// </summary>
    public string Host { get; set; } = "127.0.0.1";

    /// <summary>
    /// Publisher port number the broker is listening on.
    /// </summary>
    public int Port { get; set; } = 9096;

    /// <summary>
    /// Subscriber port number the broker is listening on.
    /// </summary>
    public int SubscriberPort { get; set; } = 9098;

    /// <summary>
    /// Maximum number of messages to queue before blocking.
    /// </summary>
    public uint MaxQueueSize { get; set; } = 10000;
}



