namespace BddE2eTests.Configuration.TestEvents;

public class TestEvent
{
    public string Message { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public int? Priority { get; set; }
}