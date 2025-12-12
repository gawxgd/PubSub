using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Configuration.Exceptions;
using Subscriber.Configuration.Options;
using Subscriber.Domain;
using Subscriber.Outbound.Adapter;

namespace Subscriber.Configuration;

public sealed class SubscriberFactory() : ISubscriberFactory
{
    private const int MinPort = 1;
    private const int MaxPort = 65535;
    private const string AllowedUriScheme = "messageBroker";
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<SubscriberFactory>(LogSource.MessageBroker);

    public ISubscriber CreateSubscriber(SubscriberOptions options, Func<string, Task>? messageHandler = null)
    {
        var (host, port, topic, minLen, maxLen, poll, retry) = ValidateOptions(options);
        var requestChannel = Channel.CreateBounded<byte[]>(
            new BoundedChannelOptions(options.MaxQueueSize)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = true
            });

        var responseChannel = Channel.CreateBounded<byte[]>(
            new BoundedChannelOptions(options.MaxQueueSize)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });

        var connection = new TcpSubscriberConnection(host, port, requestChannel, responseChannel);

        return new TcpSubscriber(
            topic,
            minLen,
            maxLen,
            poll,
            retry,
            connection,
            responseChannel,  // respondChannel in TcpSubscriber
            requestChannel,   // requestChannel in TcpSubscriber
            messageHandler);

    }

    private (string host, int port, string topic, int minLen, int maxLen, TimeSpan poll, uint retry)
        ValidateOptions(SubscriberOptions options)
    {
        var uri = options.MessageBrokerConnectionUri;

        if (!uri.IsAbsoluteUri)
        {
            Logger.LogError( $"{options.MessageBrokerConnectionUri} is not an absolute URI.");
            throw new SubscriberFactoryException("URI must be absolute", SubscriberFactoryErrorCode.InvalidUri);
        }

        if (!string.Equals(uri.Scheme, AllowedUriScheme, StringComparison.OrdinalIgnoreCase))
        {
            Logger.LogError($"{options.MessageBrokerConnectionUri.Scheme} is not a valid scheme.");
            throw new SubscriberFactoryException("Unsupported URI scheme", SubscriberFactoryErrorCode.UnsupportedScheme);
        }

        if (uri.Port is < MinPort or > MaxPort)
        {
            Logger.LogError( $"{options.Port} is not a valid port.");
            throw new SubscriberFactoryException("Invalid port", SubscriberFactoryErrorCode.InvalidPort);
        }

        if (string.IsNullOrWhiteSpace(options.Topic))
        {
            Logger.LogError( $"{options.Topic} is not a topic.");
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
