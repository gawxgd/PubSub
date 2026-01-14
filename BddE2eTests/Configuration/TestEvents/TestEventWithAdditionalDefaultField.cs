using System.ComponentModel;
using Shared.Domain.Avro;

namespace BddE2eTests.Configuration.TestEvents;

[AvroRecordName("TestEvent", Namespace = "BddE2eTests.TestEvents")]
public class TestEventWithAdditionalDefaultField : ITestEvent
{
    public string Message { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;

    // We want the DEFAULT to exist in the Avro schema (backward-compat), not in runtime logic.
    // Schema generation should pick this up (or we will extend schema generation accordingly).
    [DefaultValue(0)]
    public int Priority { get; set; }
}

