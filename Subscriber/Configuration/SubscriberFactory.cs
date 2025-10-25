using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using Subscriber.Configuration.Exceptions;
using Subscriber.Domain;
using Subscriber.Outbound.Adapter;

namespace Subscriber.Configuration;

public sealed class SubscriberFactory(ILogger logger) : ISubscriberFactory
{
    private static ILogger _logger;
    private const int MinPort = 1;
    private const int MaxPort = 65535;
    private const string AllowedUriScheme = "messageBroker";

    public ISubscriber CreateSubscriber(SubscriberOptions options, Func<string, Task>? messageHandler = null)
    {
        var (host, port, topic, minLen, maxLen, poll, retry) = ValidateOptions(options);
        var channel = Channel.CreateUnbounded<byte[]>();
        var connection = new TcpSubscriberConnection(host, port, channel.Writer, _logger);

        return new TcpSubscriber(
            topic,
            minLen,
            maxLen,
            poll,
            retry,
            connection,
            logger,
            messageHandler);

    }

    private static (string host, int port, string topic, int minLen, int maxLen, TimeSpan poll, uint retry)
        ValidateOptions(SubscriberOptions options)
    {
        var uri = options.MessageBrokerConnectionUri;

        if (!uri.IsAbsoluteUri)
        {
            _logger.LogError(LogSource.Subscriber, $"{options.MessageBrokerConnectionUri} is not an absolute URI.");
            throw new SubscriberFactoryException("URI must be absolute", SubscriberFactoryErrorCode.InvalidUri);
        }

        if (!string.Equals(uri.Scheme, AllowedUriScheme, StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogError(LogSource.Subscriber, $"{options.MessageBrokerConnectionUri.Scheme} is not a valid scheme.");
            throw new SubscriberFactoryException("Unsupported URI scheme", SubscriberFactoryErrorCode.UnsupportedScheme);
        }

        if (uri.Port is < MinPort or > MaxPort)
        {
            _logger.LogError(LogSource.Subscriber, $"{options.Port} is not a valid port.");
            throw new SubscriberFactoryException("Invalid port", SubscriberFactoryErrorCode.InvalidPort);
        }

        if (string.IsNullOrWhiteSpace(options.Topic))
        {
            _logger.LogError(LogSource.Subscriber, $"{options.Topic} is not a topic.");
            throw new SubscriberFactoryException("Topic is required", SubscriberFactoryErrorCode.MissingTopic);
        }

        return (
            uri.Host,
            uri.Port,
            options.Topic,
            options.MinMessageLength,
            options.MaxMessageLength,
            options.PollInterval,
            options.MaxRetryAttempts
        );
    }
}
