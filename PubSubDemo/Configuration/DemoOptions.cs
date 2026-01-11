namespace PubSubDemo.Configuration;

/// <summary>
/// Configuration options for the demo service behavior.
/// </summary>
public sealed class DemoOptions
{
    /// <summary>
    /// Interval between messages in milliseconds.
    /// </summary>
    public int MessageInterval { get; set; } = 1000;

    /// <summary>
    /// Prefix for demo messages.
    /// </summary>
    public string MessagePrefix { get; set; } = "Demo";

    /// <summary>
    /// Number of messages to send in a batch. TODO: do we need this?
    /// </summary> 
    public int BatchSize { get; set; } = 10;
    
    /// <summary>
    /// Topic for demo TCP Publisher
    /// </summary>
    public string Topic { get; set; } = "DemoTopic";

    /// <summary>
    /// Topics to publish to. If set (non-empty), demo will publish round-robin across these topics.
    /// Falls back to <see cref="Topic"/> when not provided.
    /// </summary>
    public string[] Topics { get; set; } = new[] { "default", "metrics", "audit" };
    
    /// <summary>
    /// Maximum size of a batch measured in bytes
    /// </summary>
    public int BatchMaxBytes { get; set; } = 65536;
    
    /// <summary>
    /// Maximum time that a batch waits to be sent after its creation
    /// </summary>
    public TimeSpan BatchMaxDelay { get; set; } = TimeSpan.FromSeconds(10);
    
}



