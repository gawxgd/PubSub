namespace PerformanceTests.Models;

public sealed class TestMessage
{
    public TestMessage()
    {

    }

    public int Id { get; set; }
    public long Timestamp { get; set; }
    public string Content { get; set; } = string.Empty;
    public string Source { get; set; } = "PerformanceTest";
    public long SequenceNumber { get; set; }
}

