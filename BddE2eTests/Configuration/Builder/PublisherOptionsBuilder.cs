using BddE2eTests.Configuration.Options;
using Publisher.Configuration.Options;

namespace BddE2eTests.Configuration.Builder;

public class PublisherOptionsBuilder(PublisherTestOptions options)
{
    private string _brokerHost = options.BrokerHost;
    private int _brokerPort = options.BrokerPort;
    private string _topic = options.Topic;
    private uint _maxPublisherQueueSize = options.MaxPublisherQueueSize;
    private uint _maxSendAttempts = options.MaxSendAttempts;
    private uint _maxRetryAttempts = options.MaxRetryAttempts;
    private string _schemaRegistryHost = options.SchemaRegistryHost;
    private int _schemaRegistryPort = options.SchemaRegistryPort;
    private TimeSpan _schemaRegistryTimeout = TimeSpan.FromSeconds(options.SchemaRegistryTimeoutSeconds);
    private int _batchMaxBytes = options.BatchMaxBytes;
    private TimeSpan _batchMaxDelay = TimeSpan.FromMilliseconds(options.BatchMaxDelayMs);
    
    public PublisherOptionsBuilder WithBrokerHost(string host)
    {
        _brokerHost = host ?? throw new ArgumentNullException(nameof(host));
        return this;
    }

    public PublisherOptionsBuilder WithBrokerPort(int port)
    {
        if (port <= 0) throw new ArgumentException("Port must be positive", nameof(port));
        _brokerPort = port;
        return this;
    }

    public PublisherOptionsBuilder WithTopic(string topic)
    {
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
        return this;
    }

    public PublisherOptionsBuilder WithMaxPublisherQueueSize(uint size)
    {
        if (size == 0) throw new ArgumentException("Queue size must be positive", nameof(size));
        _maxPublisherQueueSize = size;
        return this;
    }

    public PublisherOptionsBuilder WithMaxSendAttempts(uint attempts)
    {
        if (attempts == 0) throw new ArgumentException("Max send attempts must be positive", nameof(attempts));
        _maxSendAttempts = attempts;
        return this;
    }

    public PublisherOptionsBuilder WithMaxRetryAttempts(uint attempts)
    {
        if (attempts == 0) throw new ArgumentException("Max retry attempts must be positive", nameof(attempts));
        _maxRetryAttempts = attempts;
        return this;
    }

    public PublisherOptionsBuilder WithSchemaRegistryHost(string host)
    {
        _schemaRegistryHost = host ?? throw new ArgumentNullException(nameof(host));
        return this;
    }

    public PublisherOptionsBuilder WithSchemaRegistryPort(int port)
    {
        if (port <= 0) throw new ArgumentException("Port must be positive", nameof(port));
        _schemaRegistryPort = port;
        return this;
    }

    public PublisherOptionsBuilder WithSchemaRegistryTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero) throw new ArgumentException("Timeout must be positive", nameof(timeout));
        _schemaRegistryTimeout = timeout;
        return this;
    }

    public PublisherOptionsBuilder WithBatchMaxBytes(int bytes)
    {
        if (bytes <= 0) throw new ArgumentException("Batch max bytes must be positive", nameof(bytes));
        _batchMaxBytes = bytes;
        return this;
    }

    public PublisherOptionsBuilder WithBatchMaxDelay(TimeSpan delay)
    {
        if (delay <= TimeSpan.Zero) throw new ArgumentException("Batch max delay must be positive", nameof(delay));
        _batchMaxDelay = delay;
        return this;
    }

    public PublisherOptions Build()
    {
        return new PublisherOptions(
            MessageBrokerConnectionUri: new Uri($"messageBroker:
            SchemaRegistryConnectionUri: new Uri($"http:
            SchemaRegistryTimeout: _schemaRegistryTimeout,
            Topic: _topic,
            MaxPublisherQueueSize: _maxPublisherQueueSize,
            MaxSendAttempts: _maxSendAttempts,
            MaxRetryAttempts: _maxRetryAttempts,
            BatchMaxBytes: _batchMaxBytes,
            BatchMaxDelay: _batchMaxDelay
        );
    }
}
