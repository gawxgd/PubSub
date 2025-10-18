namespace Publisher.Configuration.Options;

public sealed record PublisherOptions(Uri MessageBrokerConnectionUri, uint maxPublisherQueueSize)
{
}