namespace BddE2eTests.Configuration.Options;

public sealed record TestOptions(
    PublisherTestOptions Publisher,
    SubscriberTestOptions Subscriber,
    SchemaRegistryTestOptions SchemaRegistry
);
