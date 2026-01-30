using BddE2eTests.Configuration.Options;
using Subscriber.Configuration.Options;

namespace BddE2eTests.Configuration.Builder;

public class SubscriberOptionsBuilder(SubscriberTestOptions options)
{
    private string _brokerHost = options.BrokerHost;
    private int _brokerPort = options.BrokerPort;
    private string _topic = options.Topic;
    private TimeSpan _pollInterval = TimeSpan.FromMilliseconds(options.PollIntervalMs);
    private uint _maxRetryAttempts = options.MaxRetryAttempts;
    private string _schemaRegistryHost = options.SchemaRegistryHost;
    private int _schemaRegistryPort = options.SchemaRegistryPort;
    private TimeSpan _schemaRegistryTimeout = TimeSpan.FromSeconds(options.SchemaRegistryTimeoutSeconds);
    
    public SubscriberOptionsBuilder WithBrokerHost(string host)
    {
        _brokerHost = host ?? throw new ArgumentNullException(nameof(host));
        return this;
    }

    public SubscriberOptionsBuilder WithBrokerPort(int port)
    {
        if (port <= 0) throw new ArgumentException("Port must be positive", nameof(port));
        _brokerPort = port;
        return this;
    }

    public SubscriberOptionsBuilder WithTopic(string topic)
    {
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
        return this;
    }

    public SubscriberOptionsBuilder WithPollInterval(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero) throw new ArgumentException("Poll interval must be positive", nameof(interval));
        _pollInterval = interval;
        return this;
    }

    public SubscriberOptionsBuilder WithMaxRetryAttempts(uint attempts)
    {
        if (attempts == 0) throw new ArgumentException("Max retry attempts must be positive", nameof(attempts));
        _maxRetryAttempts = attempts;
        return this;
    }

    public SubscriberOptionsBuilder WithSchemaRegistryHost(string host)
    {
        _schemaRegistryHost = host ?? throw new ArgumentNullException(nameof(host));
        return this;
    }

    public SubscriberOptionsBuilder WithSchemaRegistryPort(int port)
    {
        if (port <= 0) throw new ArgumentException("Port must be positive", nameof(port));
        _schemaRegistryPort = port;
        return this;
    }

    public SubscriberOptionsBuilder WithSchemaRegistryTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero) throw new ArgumentException("Timeout must be positive", nameof(timeout));
        _schemaRegistryTimeout = timeout;
        return this;
    }

    public SubscriberOptions Build()
    {
        return new SubscriberOptions(
            MessageBrokerConnectionUri: new Uri($"messageBroker:
            SchemaRegistryConnectionUri: new Uri($"http:
            Host: _brokerHost,
            Port: _brokerPort,
            Topic: _topic,
            MinMessageLength: 0,
            MaxMessageLength: int.MaxValue,
            MaxQueueSize: 65536,
            PollInterval: _pollInterval,
            SchemaRegistryTimeout: _schemaRegistryTimeout,
            MaxRetryAttempts: _maxRetryAttempts
        );
    }
}
