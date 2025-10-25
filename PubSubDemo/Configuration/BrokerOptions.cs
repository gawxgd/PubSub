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
    /// Port number the broker is listening on.
    /// </summary>
    public int Port { get; set; } = 9096;

    /// <summary>
    /// Maximum number of messages to queue before blocking.
    /// </summary>
    public uint MaxQueueSize { get; set; } = 10000;
}



