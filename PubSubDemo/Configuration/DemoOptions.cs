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
    /// Number of messages to send in a batch.
    /// </summary>
    public int BatchSize { get; set; } = 10;
}



