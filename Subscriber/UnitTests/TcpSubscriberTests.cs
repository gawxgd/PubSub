using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using Moq;
using Subscriber.Domain;
using Subscriber.Outbound.Adapter;
using Subscriber.Outbound.Exceptions;
using Xunit;

namespace Subscriber.UnitTests;

public class TcpSubscriberTests
{
    private const string Topic = "test-topic";
    private const int MinMessageLength = 1;
    private const int MaxMessageLength = 100;
    private readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(10);
    private const uint MaxRetryAttempts = 3;

    private readonly Mock<ISubscriberConnection> _connectionMock;
    private readonly Mock<ILogger> _loggerMock;
    private readonly Mock<Func<string, Task>> _messageHandlerMock;

    public TcpSubscriberTests()
    {
        _connectionMock = new Mock<ISubscriberConnection>();
        _loggerMock = new Mock<ILogger>();
        _messageHandlerMock = new Mock<Func<string, Task>>();
    }

    private TcpSubscriber CreateSubscriber()
    {
        return new TcpSubscriber(
            Topic,
            MinMessageLength,
            MaxMessageLength,
            PollInterval,
            MaxRetryAttempts,
            _connectionMock.Object,
            _loggerMock.Object,
            _messageHandlerMock.Object);
    }

    [Fact]
    public async Task CreateConnection_ShouldSucceedOnFirstAttempt()
    {
        // Arrange
        _connectionMock
            .Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var subscriber = CreateSubscriber();

        // Act
        await subscriber.CreateConnection(CancellationToken.None);

        // Assert
        _connectionMock.Verify(c => c.ConnectAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateConnection_ShouldRetryOnFailure()
    {
        // Arrange
        _connectionMock
            .SetupSequence(c => c.ConnectAsync(It.IsAny<CancellationToken>()))
            .Throws(new SubscriberConnectionException("Connection failed", null))
            .Throws(new SubscriberConnectionException("Connection failed again", null))
            .Returns(Task.CompletedTask);

        var subscriber = CreateSubscriber();

        // Act
        await subscriber.CreateConnection(CancellationToken.None);

        // Assert
        _connectionMock.Verify(c => c.ConnectAsync(It.IsAny<CancellationToken>()), Times.Exactly(3));
        _loggerMock.Verify(
            l => l.LogDebug(LogSource.Subscriber, It.Is<string>(s => s.Contains("Retry"))),
            Times.Exactly(2));
    }

    [Fact]
    public async Task CreateConnection_ShouldThrowAfterMaxRetries()
    {
        // Arrange
        _connectionMock
            .Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>()))
            .Throws(new SubscriberConnectionException("Connection failed", null));

        var subscriber = CreateSubscriber();

        // Act & Assert
        await Assert.ThrowsAsync<SubscriberConnectionException>(
            () => subscriber.CreateConnection(CancellationToken.None));

        _connectionMock.Verify(c => c.ConnectAsync(It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    [Fact]
    public async Task ReceiveAsync_ShouldIgnoreMessageWithInvalidLength()
    {
        // Arrange
        var subscriber = CreateSubscriber();
        var message = Encoding.UTF8.GetBytes(new string('x', MaxMessageLength + 10));

        // Act
        await subscriber.ReceiveAsync(message, CancellationToken.None);

        // Assert
        _loggerMock.Verify(
            l => l.LogError(LogSource.Subscriber, It.Is<string>(s => s.Contains("Invalid message length"))),
            Times.Once);
        _messageHandlerMock.Verify(h => h.Invoke(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldIgnoreMessageWithWrongTopic()
    {
        // Arrange
        var subscriber = CreateSubscriber();
        var message = Encoding.UTF8.GetBytes("wrong-topic:payload");

        // Act
        await subscriber.ReceiveAsync(message, CancellationToken.None);

        // Assert
        _loggerMock.Verify(
            l => l.LogError(LogSource.Subscriber, It.Is<string>(s => s.Contains("wrong topic"))),
            Times.Once);
        _messageHandlerMock.Verify(h => h.Invoke(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldProcessValidMessage()
    {
        // Arrange
        var subscriber = CreateSubscriber();
        var payload = "test-payload";
        var message = Encoding.UTF8.GetBytes($"{Topic}:{payload}");

        // Act
        await subscriber.ReceiveAsync(message, CancellationToken.None);

        // Assert
        _messageHandlerMock.Verify(h => h.Invoke(payload), Times.Once);
        _loggerMock.Verify(
            l => l.LogInfo(LogSource.Subscriber, It.Is<string>(s => s.Contains(payload))),
            Times.Once);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldHandleHandlerException()
    {
        // Arrange
        var subscriber = CreateSubscriber();
        var payload = "test-payload";
        var message = Encoding.UTF8.GetBytes($"{Topic}:{payload}");
        var exception = new Exception("Handler error");

        _messageHandlerMock
            .Setup(h => h.Invoke(payload))
            .Throws(exception);

        // Act
        await subscriber.ReceiveAsync(message, CancellationToken.None);

        // Assert
        _loggerMock.Verify(
            l => l.LogError(LogSource.Subscriber, exception.Message),
            Times.Once);
    }

    [Fact]
    public async Task StartAsync_ShouldConnectSuccessfully()
    {
        // Arrange
        var subscriber = CreateSubscriber();

        _connectionMock
            .Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        await subscriber.CreateConnection(CancellationToken.None);

        // Assert
        _connectionMock.Verify(c => c.ConnectAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_ShouldCleanupResources()
    {
        // Arrange
        var subscriber = CreateSubscriber();
        var channel = GetInboundChannel(subscriber);

        _connectionMock
            .Setup(c => c.DisconnectAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        await subscriber.DisposeAsync();

        // Assert
        _connectionMock.Verify(c => c.DisconnectAsync(It.IsAny<CancellationToken>()), Times.Once);
        Assert.True(channel.Reader.Completion.IsCompleted);
    }

    private static Channel<byte[]> GetInboundChannel(TcpSubscriber subscriber)
    {
        var field = typeof(TcpSubscriber).GetField("_inboundChannel", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        return (Channel<byte[]>)field!.GetValue(subscriber)!;
    }
}
