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


    public ITransportPublisher CreatePublisher(PublisherOptions options)
    {
        var (host, port, maxQueueSize, maxSendAttempts, maxRetryAttempts) = ValidateOptions(options);

        return new TcpTransportPublisher(host, port, maxQueueSize, maxSendAttempts, maxRetryAttempts);
    }

    private static (string host, int port, uint maxQueueSize, uint maxSendAttempts, uint maxRetryAttempts)
        ValidateOptions(PublisherOptions options)
    {
        var connectionUri = options.MessageBrokerConnectionUri;

        if (!connectionUri.IsAbsoluteUri)
        {
            throw new PublisherFactoryException(
                $"Broker URI '{connectionUri}' must be absolute.",
                PublisherFactoryErrorCode.InvalidUri);
        }

        if (!string.Equals(AllowedUriScheme, connectionUri.Scheme.ToLowerInvariant()))
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
                $"Max transportPublisher queue size '{options.MaxPublisherQueueSize}' exceeds limit of {MaxPublisherQueueSize}.",
                PublisherFactoryErrorCode.QueueSizeExceeded);
        }

        return (connectionUri.Host, connectionUri.Port, options.MaxPublisherQueueSize, options.MaxSendAttempts,
            options.MaxRetryAttempts);
    }
}