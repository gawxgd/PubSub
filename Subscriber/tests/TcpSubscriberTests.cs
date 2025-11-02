using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using Moq;
using Subscriber.Domain;
using Subscriber.Outbound.Adapter;
using Subscriber.Outbound.Exceptions;
using Xunit;

namespace Subscriber.Tests;

public class TcpSubscriberTests
{
    private const string Topic = "test-topic";
    private const int MinMessageLength = 1;
    private const int MaxMessageLength = 100;
    private readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(10);
    private const uint MaxRetryAttempts = 3;

    private readonly Mock<ISubscriberConnection> _connectionMock = new();
    private readonly Mock<ILogger> _loggerMock = new();
    private readonly Mock<Func<string, Task>> _messageHandlerMock = new();

    private Channel<byte[]> CreateBoundedChannel(int capacity = 100) =>
        Channel.CreateBounded<byte[]>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

    private TcpSubscriber CreateSubscriber(Channel<byte[]> channel) =>
        new TcpSubscriber(
            Topic,
            MinMessageLength,
            MaxMessageLength,
            PollInterval,
            MaxRetryAttempts,
            _connectionMock.Object,
            channel,
            _messageHandlerMock.Object);

    [Fact]
    public async Task CreateConnection_ShouldSucceedOnFirstAttempt()
    {
        _connectionMock.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        var subscriber = CreateSubscriber(CreateBoundedChannel());

        await ((ISubscriber)subscriber).CreateConnection();

        _connectionMock.Verify(c => c.ConnectAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConnection_ShouldRetryOnFailure()
    {
        _connectionMock.SetupSequence(c => c.ConnectAsync(It.IsAny<CancellationToken>()))
            .Throws(new SubscriberConnectionException("fail", null))
            .Throws(new SubscriberConnectionException("fail again", null))
            .Returns(Task.CompletedTask);

        var subscriber = CreateSubscriber(CreateBoundedChannel());

        await ((ISubscriber)subscriber).CreateConnection();

        _connectionMock.Verify(c => c.ConnectAsync(It.IsAny<CancellationToken>()), Times.Exactly(3));
        _loggerMock.Verify(l => l.LogDebug(LogSource.Subscriber, It.Is<string>(s => s.Contains("Retry"))), Times.Exactly(2));
    }

    [Fact]
    public async Task CreateConnection_ShouldThrowAfterMaxRetries()
    {
        _connectionMock.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>()))
            .Throws(new SubscriberConnectionException("fail", null));

        var subscriber = CreateSubscriber(CreateBoundedChannel());

        await Assert.ThrowsAsync<SubscriberConnectionException>(() =>
            ((ISubscriber)subscriber).CreateConnection());

        _connectionMock.Verify(c => c.ConnectAsync(It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    [Fact]
    public async Task ReceiveAsync_ShouldIgnoreInvalidLength()
    {
        var subscriber = CreateSubscriber(CreateBoundedChannel());
        var message = Encoding.UTF8.GetBytes(new string('x', MaxMessageLength + 10));

        await subscriber.ReceiveAsync(message);

        _loggerMock.Verify(l => l.LogError(LogSource.Subscriber, It.Is<string>(s => s.Contains("Invalid message length"))));
        _messageHandlerMock.Verify(h => h.Invoke(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldIgnoreWrongTopic()
    {
        var subscriber = CreateSubscriber(CreateBoundedChannel());
        var message = Encoding.UTF8.GetBytes("wrong-topic:payload");

        await subscriber.ReceiveAsync(message);

        _loggerMock.Verify(l => l.LogError(LogSource.Subscriber, It.Is<string>(s => s.Contains("wrong topic"))));
        _messageHandlerMock.Verify(h => h.Invoke(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldProcessValidMessage()
    {
        var subscriber = CreateSubscriber(CreateBoundedChannel());
        var payload = "test-payload";
        var message = Encoding.UTF8.GetBytes($"{Topic}:{payload}");

        await subscriber.ReceiveAsync(message);

        _messageHandlerMock.Verify(h => h.Invoke(payload), Times.Once);
        _loggerMock.Verify(l => l.LogInfo(LogSource.Subscriber, It.Is<string>(s => s.Contains(payload))), Times.Once);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldHandleHandlerException()
    {
        var subscriber = CreateSubscriber(CreateBoundedChannel());
        var payload = "test-payload";
        var message = Encoding.UTF8.GetBytes($"{Topic}:{payload}");
        var exception = new Exception("Handler error");

        _messageHandlerMock.Setup(h => h.Invoke(payload)).Throws(exception);

        await subscriber.ReceiveAsync(message);

        _loggerMock.Verify(l => l.LogError(LogSource.Subscriber, exception.Message), Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldProcessMessageAndExit()
    {
        var channel = CreateBoundedChannel();
        var subscriber = CreateSubscriber(channel);

        var payload = "test-payload";
        var message = Encoding.UTF8.GetBytes($"{Topic}:{payload}");
        await channel.Writer.WriteAsync(message);
        channel.Writer.Complete(); 

        _connectionMock.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        _connectionMock.Setup(c => c.DisconnectAsync()).Returns(Task.CompletedTask);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2)); 

        await subscriber.StartAsync();

        _messageHandlerMock.Verify(h => h.Invoke(payload), Times.Once);
        _connectionMock.Verify(c => c.DisconnectAsync(), Times.Once);
    }

    
    [Fact]
    public async Task DisposeAsync_ShouldCleanupResources()
    {
        var channel = CreateBoundedChannel();
        var subscriber = CreateSubscriber(channel);

        _connectionMock.Setup(c => c.DisconnectAsync()).Returns(Task.CompletedTask);

        await subscriber.DisposeAsync();

        _connectionMock.Verify(c => c.DisconnectAsync(), Times.Once);
        Assert.True(channel.Reader.Completion.IsCompleted);
    }

}
