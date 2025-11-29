using Publisher.Configuration.Exceptions;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using Publisher.Outbound.Adapter;

namespace Publisher.Configuration;

public sealed class PublisherFactory : IPublisherFactory
{
    private const int MinPort = 1;
    private const int MaxPort = 65535;
    private const uint MaxPublisherQueueSize = 65536; // think about value
    private const string AllowedUriScheme = "messageBroker";


    public IPublisher CreatePublisher(PublisherOptions options)
    {
        var (host, port, topic, maxQueueSize, maxSendAttempts, maxRetryAttempts) = ValidateOptions(options);

        return new TcpPublisher(host, port, topic, maxQueueSize, maxSendAttempts, maxRetryAttempts);
    }

    private static (string host, int port, string topic, uint maxQueueSize, uint maxSendAttempts, uint maxRetryAttempts)
        ValidateOptions(PublisherOptions options)
    {
        var connectionUri = options.MessageBrokerConnectionUri;

        if (!connectionUri.IsAbsoluteUri)
        {
            throw new PublisherFactoryException(
                $"Broker URI '{connectionUri}' must be absolute.",
                PublisherFactoryErrorCode.InvalidUri);
        }

        if (!string.Equals(connectionUri.Scheme, AllowedUriScheme, StringComparison.OrdinalIgnoreCase))
        {
            throw new PublisherFactoryException(
                $"Unsupported URI scheme '{connectionUri.Scheme}'. Allowed: '{AllowedUriScheme}'.",
                PublisherFactoryErrorCode.UnsupportedScheme);
        }

        if (connectionUri.Port is < MinPort or > MaxPort)
        {
            throw new PublisherFactoryException(
                $"Port '{connectionUri.Port}' must be between {MinPort} and {MaxPort}.",
                PublisherFactoryErrorCode.InvalidPort);
        }

        if (options.MaxPublisherQueueSize > MaxPublisherQueueSize)
        {
            throw new PublisherFactoryException(
                $"Max publisher queue size '{options.MaxPublisherQueueSize}' exceeds limit of {MaxPublisherQueueSize}.",
                PublisherFactoryErrorCode.QueueSizeExceeded);
        }

        if (string.IsNullOrWhiteSpace(options.Topic))
        {
            throw new PublisherFactoryException(
                "Topic cannot be null or empty.",
                PublisherFactoryErrorCode.InvalidTopic);
        }

        return (connectionUri.Host, connectionUri.Port, options.Topic, options.MaxPublisherQueueSize, options.MaxSendAttempts,
            options.MaxRetryAttempts);
    }
}