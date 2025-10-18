using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using Publisher.Outbound.Adapter;

namespace Publisher.Configuration;

public class PublisherFactory
{
    private const int MinPort = 1;
    private const int MaxPort = 65535;
    private const uint MaxPublisherQueueSize = 65536; // think about value
    private const string AllowedUriScheme = "messageBroker";


    public static IPublisher CreateTcpPublisher(PublisherOptions options)
    {
        var (host, port, maxQueueSize) = ValidateOptions(options);

        return new TcpPublisher(host, port, maxQueueSize);
    }

    private static (string host, int port, uint maxQueueSize ) ValidateOptions(PublisherOptions options)
    {
        var connectionUri = options.MessageBrokerConnectionUri;

        if (!connectionUri.IsAbsoluteUri)
        {
            throw new ArgumentException("Broker URI must be absolute.", nameof(connectionUri));
        }

        if (!string.Equals(AllowedUriScheme, connectionUri.Scheme.ToLowerInvariant()))
        {
            throw new ArgumentException(
                $"Unsupported URI scheme '{connectionUri.Scheme}'. Allowed: {string.Join(", ", AllowedUriScheme)}.",
                nameof(connectionUri));
        }

        if (connectionUri.Port is < MinPort or > MaxPort)
        {
            throw new ArgumentOutOfRangeException(nameof(connectionUri),
                "Port must be between 1 and 65535.");
        }

        if (options.maxPublisherQueueSize > MaxPublisherQueueSize)
        {
            throw new ArgumentOutOfRangeException(nameof(options.maxPublisherQueueSize),
                "Max publisher queue size must be less than or equal to 65535.");
        }

        return (connectionUri.Host, connectionUri.Port, options.maxPublisherQueueSize);
    }
}