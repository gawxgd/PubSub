namespace PerformanceTests.Models;

/// <summary>
/// Simple test message for performance testing.
/// </summary>
public sealed class TestMessage
{
    public TestMessage()
    {
        // Required for new() constraint
    }

    public int Id { get; set; }
    public long Timestamp { get; set; } // Unix timestamp in milliseconds (compatible with Avro long type)
    public string Content { get; set; } = string.Empty;
    public string Source { get; set; } = "PerformanceTest";
    public long SequenceNumber { get; set; }
}

