namespace BddE2eTests.Configuration.TestEvents;

public static class TestEventTypeResolver
{
    public static Type Resolve(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
        {
            throw new ArgumentException("Type name must be provided", nameof(typeName));
        }

        return typeName.Trim() switch
        {
            nameof(TestEvent) => typeof(TestEvent),
            nameof(TestEventWithAdditionalField) => typeof(TestEventWithAdditionalField),
            nameof(TestEventWithAdditionalDefaultField) => typeof(TestEventWithAdditionalDefaultField),
            _ => throw new ArgumentException(
                $"Unknown event type '{typeName}'. Known types: {nameof(TestEvent)}, {nameof(TestEventWithAdditionalField)}, {nameof(TestEventWithAdditionalDefaultField)}",
                nameof(typeName))
        };
    }
}

