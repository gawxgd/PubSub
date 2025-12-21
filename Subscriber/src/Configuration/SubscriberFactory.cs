using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Record;
using Shared.Domain.Port.SchemaRegistryClient;
using Subscriber.Configuration.Exceptions;
using Subscriber.Configuration.Options;
using Subscriber.Domain;
using Subscriber.Domain.UseCase;
using Subscriber.Outbound.Adapter;

namespace Subscriber.Configuration;

public sealed class SubscriberFactory<T>(ISchemaRegistryClient schemaRegistryClient) : ISubscriberFactory<T> where T : new()
{
    private const int MinPort = 1;
    private const int MaxPort = 65535;
    private const string AllowedUriScheme = "messageBroker";
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<SubscriberFactory<T>>(LogSource.MessageBroker);

    public ISubscriber<T> CreateSubscriber(SubscriberOptions options, Func<T, Task> messageHandler)
    {
        ArgumentNullException.ThrowIfNull(messageHandler);
        
        var (host, port, topic, poll, retry) = ValidateOptions(options);

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

        var batchReader = CreateBatchReader();
        var deserializer = new AvroDeserializer<T>();
        var deserializeBatchUseCase = new DeserializeBatchUseCase<T>(batchReader, deserializer);
        var processMessageUseCase = new ProcessMessageUseCase<T>(deserializeBatchUseCase, schemaRegistryClient, messageHandler, topic, batchReader);

        return new TcpSubscriber<T>(
            topic,
            poll,
            retry,
            connection,
            responseChannel,
            requestChannel,
            processMessageUseCase);
    }

    private static ILogRecordBatchReader CreateBatchReader()
    {
        var recordReader = new LogRecordBinaryReader();
        var compressor = new NoopCompressor();
        return new LogRecordBatchBinaryReader(recordReader, compressor, Encoding.UTF8);
    }

    private (string host, int port, string topic, TimeSpan poll, uint retry)
        ValidateOptions(SubscriberOptions options)
    {
        var uri = options.MessageBrokerConnectionUri;

        if (!uri.IsAbsoluteUri)
        {
            Logger.LogError($"{options.MessageBrokerConnectionUri} is not an absolute URI.");
            throw new SubscriberFactoryException("URI must be absolute", SubscriberFactoryErrorCode.InvalidUri);
        }

        if (!string.Equals(uri.Scheme, AllowedUriScheme, StringComparison.OrdinalIgnoreCase))
        {
            Logger.LogError($"{options.MessageBrokerConnectionUri.Scheme} is not a valid scheme.");
            throw new SubscriberFactoryException("Unsupported URI scheme", SubscriberFactoryErrorCode.UnsupportedScheme);
        }

        if (uri.Port is < MinPort or > MaxPort)
        {
            Logger.LogError($"{options.Port} is not a valid port.");
            throw new SubscriberFactoryException("Invalid port", SubscriberFactoryErrorCode.InvalidPort);
        }

        if (string.IsNullOrWhiteSpace(options.Topic))
        {
            Logger.LogError($"{options.Topic} is not a topic.");
            throw new SubscriberFactoryException("Topic is required", SubscriberFactoryErrorCode.MissingTopic);
        }

        return (
            uri.Host,
            uri.Port,
            options.Topic,
            options.PollInterval,
            options.MaxRetryAttempts
        );
    }
}
