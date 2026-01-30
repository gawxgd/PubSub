using System.ComponentModel;
using Shared.Domain.Avro;

namespace BddE2eTests.Configuration.TestEvents;

[AvroRecordName("TestEvent", Namespace = "BddE2eTests.TestEvents")]
public class TestEventWithAdditionalDefaultField : ITestEvent
{
    public string Message { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;

    [DefaultValue(0)]
    public int Priority { get; set; }
}

