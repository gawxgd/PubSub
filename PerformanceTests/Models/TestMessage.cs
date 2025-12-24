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
    public DateTimeOffset Timestamp { get; set; }
    public string Content { get; set; } = string.Empty;
    public string Source { get; set; } = "PerformanceTest";
    public long SequenceNumber { get; set; }
}

