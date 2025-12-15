using Publisher.Configuration.Exceptions;
using Publisher.Configuration.Options;
using Publisher.Domain.Logic;
using Publisher.Domain.Port;
using Publisher.Domain.Service;
using Publisher.Outbound.Adapter;

namespace Publisher.Configuration;

public sealed class PublisherFactory<T> : IPublisherFactory<T>
{
    private const int MinPort = 1;
    private const int MaxPort = 65535;
    private const uint MaxPublisherQueueSize = 65536; // think about value
    private const string AllowedUriScheme = "messageBroker";


    public IPublisher<T> CreatePublisher(PublisherOptions options)
    {
        ValidateOptions(options);

        var avroSerializer = new AvroSerializer<T>();
        var schemaClient = new Sch

        var serializeMessageUseCase = new SerializeMessageUseCase<T>(avroSerializer, schemaClient, options.Topic);

        return new TcpPublisher<T>(options, serializeMessageUseCase);
    }


    private void ValidateOptions(PublisherOptions options)
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
                $"Max Publisher queue size '{options.MaxPublisherQueueSize}' exceeds limit of {MaxPublisherQueueSize}.",
                PublisherFactoryErrorCode.QueueSizeExceeded);
        }

        if (options.BatchMaxBytes <= 0)
        {
            throw new PublisherFactoryException(
                "BatchMaxBytes must be > 0.",
                PublisherFactoryErrorCode.InvalidBatchConfig);
        }

        if (options.BatchMaxDelay <= TimeSpan.Zero)
        {
            throw new PublisherFactoryException(
                "BatchMaxDelay must be > 0.",
                PublisherFactoryErrorCode.InvalidBatchConfig);
        }

        if (string.IsNullOrWhiteSpace(options.Topic))
        {
            throw new PublisherFactoryException(
                "Topic cannot be null or empty.",
                PublisherFactoryErrorCode.InvalidTopic);
        }
    }
}