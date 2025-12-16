using Publisher.Configuration.Options;

namespace BddE2eTests.Configuration.Builder;

public class PublisherOptionsBuilder
{
    private const string DefaultBrokerHost = "127.0.0.1";
    private const int DefaultBrokerPort = 9096;
    private const uint DefaultMaxPublisherQueueSize = 1000;
    private const uint DefaultMaxSendAttempts = 3;
    private const uint DefaultMaxRetryAttempts = 3;
    private const string UriScheme = "messageBroker";
    
    private string _brokerHost = DefaultBrokerHost;
    private int _brokerPort = DefaultBrokerPort;
    private string _topic = string.Empty;
    private uint _maxPublisherQueueSize = DefaultMaxPublisherQueueSize;
    private uint _maxSendAttempts = DefaultMaxSendAttempts;
    private uint _maxRetryAttempts = DefaultMaxRetryAttempts;

    public PublisherOptionsBuilder WithBrokerHost(string host)
    {
        _brokerHost = host;
        return this;
    }

    public PublisherOptionsBuilder WithBrokerPort(int port)
    {
        _brokerPort = port;
        return this;
    }

    public PublisherOptionsBuilder WithTopic(string topic)
    {
        _topic = topic;
        return this;
    }

    public PublisherOptionsBuilder WithMaxPublisherQueueSize(uint size)
    {
        _maxPublisherQueueSize = size;
        return this;
    }

    public PublisherOptionsBuilder WithMaxSendAttempts(uint attempts)
    {
        _maxSendAttempts = attempts;
        return this;
    }

    public PublisherOptionsBuilder WithMaxRetryAttempts(uint attempts)
    {
        _maxRetryAttempts = attempts;
        return this;
    }

    public PublisherOptions Build()
    {
        return new PublisherOptions(
            MessageBrokerConnectionUri: new Uri($"{UriScheme}://{_brokerHost}:{_brokerPort}"),
            Topic: _topic,
            MaxPublisherQueueSize: _maxPublisherQueueSize,
            MaxSendAttempts: _maxSendAttempts,
            MaxRetryAttempts: _maxRetryAttempts
        );
    }
}

