using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using Moq;
using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Port.SchemaRegistryClient;
using Subscriber.Domain;
using Subscriber.Domain.UseCase;
using Subscriber.Outbound.Adapter;
using Subscriber.Outbound.Exceptions;
using Xunit;

namespace Subscriber.Tests;

public class TestEvent
{
    public string Id { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
}

public class TcpSubscriberTests
{
    private const string Topic = "test-topic";
    private readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(10);
    private const uint MaxRetryAttempts = 3;

    private readonly Mock<ISubscriberConnection> _connectionMock = new();
    private readonly List<TestEvent> _receivedEvents = new();

    static TcpSubscriberTests()
    {
        var loggerMock = new Mock<ILogger>();
        AutoLoggerFactory.Initialize(loggerMock.Object);
    }

    private Channel<byte[]> CreateBoundedChannel(int capacity = 100) =>
        Channel.CreateBounded<byte[]>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

    private ProcessMessageUseCase<TestEvent> CreateProcessMessageUseCase()
    {
        var batchReaderMock = new Mock<ILogRecordBatchReader>();
        var deserializerMock = new Mock<IDeserializer<TestEvent>>();
        var schemaRegistryMock = new Mock<ISchemaRegistryClient>();

        var deserializeBatchUseCase = new DeserializeBatchUseCase<TestEvent>(
            batchReaderMock.Object,
            deserializerMock.Object);

        return new ProcessMessageUseCase<TestEvent>(
            deserializeBatchUseCase,
            schemaRegistryMock.Object,
            evt =>
            {
                _receivedEvents.Add(evt);
                return Task.CompletedTask;
            },
            Topic,
            batchReaderMock.Object);
    }

    private TcpSubscriber<TestEvent> CreateSubscriber(
        Channel<byte[]> responseChannel,
        Channel<byte[]> requestChannel,
        ProcessMessageUseCase<TestEvent>? processMessageUseCase = null) =>
        new TcpSubscriber<TestEvent>(
            Topic,
            PollInterval,
            MaxRetryAttempts,
            _connectionMock.Object,
            responseChannel,
            requestChannel,
            processMessageUseCase ?? CreateProcessMessageUseCase());

    [Fact]
    public async Task CreateConnection_ShouldSucceedOnFirstAttempt()
    {
        _connectionMock.Setup(c => c.ConnectAsync()).Returns(Task.CompletedTask);
        var subscriber = CreateSubscriber(CreateBoundedChannel(), CreateBoundedChannel());

        await ((ISubscriber<TestEvent>)subscriber).CreateConnection();

        _connectionMock.Verify(c => c.ConnectAsync(), Times.Once);
    }

    [Fact]
    public async Task CreateConnection_ShouldRetryOnFailure()
    {
        _connectionMock.SetupSequence(c => c.ConnectAsync())
            .ThrowsAsync(new SubscriberConnectionException("fail", null))
            .ThrowsAsync(new SubscriberConnectionException("fail again", null))
            .Returns(Task.CompletedTask);

        var subscriber = CreateSubscriber(CreateBoundedChannel(), CreateBoundedChannel());

        await ((ISubscriber<TestEvent>)subscriber).CreateConnection();

        _connectionMock.Verify(c => c.ConnectAsync(), Times.Exactly(3));
    }

    [Fact]
    public async Task CreateConnection_ShouldThrowAfterMaxRetries()
    {
        _connectionMock.Setup(c => c.ConnectAsync())
            .ThrowsAsync(new SubscriberConnectionException("fail", null));

        var subscriber = CreateSubscriber(CreateBoundedChannel(), CreateBoundedChannel());

        await Assert.ThrowsAsync<SubscriberConnectionException>(() =>
            ((ISubscriber<TestEvent>)subscriber).CreateConnection());

        _connectionMock.Verify(c => c.ConnectAsync(), Times.Exactly((int)MaxRetryAttempts));
    }

    [Fact]
    public async Task StartConnectionAsync_ShouldConnectSuccessfully()
    {
        var responseChannel = CreateBoundedChannel();
        var requestChannel = CreateBoundedChannel();
        var subscriber = CreateSubscriber(responseChannel, requestChannel);

        _connectionMock.Setup(c => c.ConnectAsync()).Returns(Task.CompletedTask);

        await subscriber.StartConnectionAsync();

        _connectionMock.Verify(c => c.ConnectAsync(), Times.Once);
    }

    [Fact]
    public async Task StartConnectionAsync_ShouldRetryOnRetriableException()
    {
        var responseChannel = CreateBoundedChannel();
        var requestChannel = CreateBoundedChannel();
        var subscriber = CreateSubscriber(responseChannel, requestChannel);

        _connectionMock.SetupSequence(c => c.ConnectAsync())
            .ThrowsAsync(new SubscriberConnectionException("fail", null, isRetriable: true))
            .Returns(Task.CompletedTask);

        await subscriber.StartConnectionAsync();

        _connectionMock.Verify(c => c.ConnectAsync(), Times.Exactly(2));
    }

    [Fact]
    public async Task DisposeAsync_ShouldCleanupResources()
    {
        var responseChannel = CreateBoundedChannel();
        var requestChannel = CreateBoundedChannel();
        var subscriber = CreateSubscriber(responseChannel, requestChannel);

        _connectionMock.Setup(c => c.DisconnectAsync()).Returns(Task.CompletedTask);

        await subscriber.DisposeAsync();

        _connectionMock.Verify(c => c.DisconnectAsync(), Times.Once);
        Assert.True(responseChannel.Reader.Completion.IsCompleted);
        Assert.True(requestChannel.Reader.Completion.IsCompleted);
    }

    [Fact]
    public async Task DisposeAsync_ShouldCompleteChannels()
    {
        var responseChannel = CreateBoundedChannel();
        var requestChannel = CreateBoundedChannel();
        var subscriber = CreateSubscriber(responseChannel, requestChannel);

        _connectionMock.Setup(c => c.DisconnectAsync()).Returns(Task.CompletedTask);

        await subscriber.DisposeAsync();

        Assert.False(responseChannel.Writer.TryWrite(new byte[] { 1 }));
        Assert.False(requestChannel.Writer.TryWrite(new byte[] { 1 }));
    }
}