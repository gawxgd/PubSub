namespace BddE2eTests.Configuration.Options;

public sealed record SchemaRegistryTestOptions(
    string Host,
    int Port,
    int TimeoutSeconds
);

