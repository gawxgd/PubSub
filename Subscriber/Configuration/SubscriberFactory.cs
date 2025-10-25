using System.Threading.Channels;
using Subscriber.Domain;
using Subscriber.Inbound.Adapter;

namespace Subscriber.Configuration;

public sealed class SubscriberFactory : ISubscriberFactory
{
    private const int MinPort = 1;
    private const int MaxPort = 65535;
    private const string AllowedUriScheme = "messageBroker";

    public ISubscriber CreateSubscriber(SubscriberOptions options, Func<string, Task>? messageHandler = null)
    {
        var (host, port, topic, minLen, maxLen, poll, retry) = ValidateOptions(options);
        var channel = Channel.CreateUnbounded<byte[]>();
        var connection = new TcpSubscriberConnection(host, port, channel.Writer);

        return new TcpSubscriber(
            topic,
            minLen,
            maxLen,
            poll,
            retry,
            connection,
            messageHandler);

    }

    private static (string host, int port, string topic, int minLen, int maxLen, TimeSpan poll, uint retry)
        ValidateOptions(SubscriberOptions options)
    {
        var uri = options.MessageBrokerConnectionUri;

        if (!uri.IsAbsoluteUri)
            throw new SubscriberFactoryException("URI must be absolute", SubscriberFactoryErrorCode.InvalidUri);

        if (!string.Equals(uri.Scheme, AllowedUriScheme, StringComparison.OrdinalIgnoreCase))
            throw new SubscriberFactoryException("Unsupported URI scheme", SubscriberFactoryErrorCode.UnsupportedScheme);

        if (uri.Port is < MinPort or > MaxPort)
            throw new SubscriberFactoryException("Invalid port", SubscriberFactoryErrorCode.InvalidPort);

        if (string.IsNullOrWhiteSpace(options.Topic))
            throw new SubscriberFactoryException("Topic is required", SubscriberFactoryErrorCode.MissingTopic);

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
