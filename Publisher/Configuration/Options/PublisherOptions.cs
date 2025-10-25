namespace Publisher.Configuration.Options;

public sealed record PublisherOptions(
    Uri MessageBrokerConnectionUri,
    uint MaxPublisherQueueSize,
    uint MaxSendAttempts,
    uint MaxRetryAttempts);