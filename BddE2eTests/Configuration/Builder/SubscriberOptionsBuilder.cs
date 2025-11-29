using Subscriber.Configuration.Options;

namespace BddE2eTests.Configuration.Builder;

public class SubscriberOptionsBuilder
{
    private const string DefaultBrokerHost = "127.0.0.1";
    private const int DefaultBrokerPort = 9096;
    private const int DefaultMinMessageLength = 1;
    private const int DefaultMaxMessageLength = 1024;
    private const int DefaultPollIntervalMs = 100;
    private const uint DefaultMaxRetryAttempts = 3;
    private const string UriScheme = "messageBroker";
    
    private string _brokerHost = DefaultBrokerHost;
    private int _brokerPort = DefaultBrokerPort;
    private string _topic = string.Empty;
    private int _minMessageLength = DefaultMinMessageLength;
    private int _maxMessageLength = DefaultMaxMessageLength;
    private TimeSpan _pollInterval = TimeSpan.FromMilliseconds(DefaultPollIntervalMs);
    private uint _maxRetryAttempts = DefaultMaxRetryAttempts;

    public SubscriberOptionsBuilder WithBrokerHost(string host)
    {
        _brokerHost = host;
        return this;
    }

    public SubscriberOptionsBuilder WithBrokerPort(int port)
    {
        _brokerPort = port;
        return this;
    }

    public SubscriberOptionsBuilder WithTopic(string topic)
    {
        _topic = topic;
        return this;
    }

    public SubscriberOptionsBuilder WithMinMessageLength(int length)
    {
        _minMessageLength = length;
        return this;
    }

    public SubscriberOptionsBuilder WithMaxMessageLength(int length)
    {
        _maxMessageLength = length;
        return this;
    }

    public SubscriberOptionsBuilder WithPollInterval(TimeSpan interval)
    {
        _pollInterval = interval;
        return this;
    }

    public SubscriberOptionsBuilder WithMaxRetryAttempts(uint attempts)
    {
        _maxRetryAttempts = attempts;
        return this;
    }

    public SubscriberOptions Build()
    {
        return new SubscriberOptions
        {
            MessageBrokerConnectionUri = new Uri($"{UriScheme}://{_brokerHost}:{_brokerPort}"),
            Topic = _topic,
            MinMessageLength = _minMessageLength,
            MaxMessageLength = _maxMessageLength,
            PollInterval = _pollInterval,
            MaxRetryAttempts = _maxRetryAttempts
        };
    }
}

