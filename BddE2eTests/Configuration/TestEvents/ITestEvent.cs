namespace BddE2eTests.Configuration.TestEvents;

public interface ITestEvent
{
    string Message { get; set; }
    string Topic { get; set; }
}

