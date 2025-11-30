using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using Moq;
using Subscriber.Domain;
using Subscriber.Outbound.Adapter;
using Subscriber.Outbound.Exceptions;
using Xunit;

public class TcpSubscriberTests
{
    private const string Topic = "test";
    private readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(10);

    private TcpSubscriber CreateSubscriber(
        out Mock<ISubscriberConnection> connMock,
        out Channel<byte[]> respondChannel,
        out Channel<byte[]> requestChannel,
        Func<string, Task>? handler = null)
    {
        connMock = new Mock<ISubscriberConnection>();
        respondChannel = Channel.CreateUnbounded<byte[]>();
        requestChannel = Channel.CreateUnbounded<byte[]>();

        return new TcpSubscriber(
            topic: Topic,
            minMessageLength: 1,
            maxMessageLength: 200,
            pollInterval: PollInterval,
            maxRetryAttempts: 3,
            connection: connMock.Object,
            respondChannel,
            requestChannel,
            messageHandler: handler
        );
    }


    [Fact]
    public async Task ReceiveAsync_Should_Call_Handler_When_Valid_Message()
    {
        // Arrange
        string payloadReceived = "";
        var subscriber = CreateSubscriber(
            out _,
            out _,
            out _,
            handler: p =>
            {
                payloadReceived = p;
                return Task.CompletedTask;
            });

        var msg = Encoding.UTF8.GetBytes("test:Hello");

        // Act
        await subscriber.ReceiveAsync(msg);

        // Assert
        Assert.Equal("Hello", payloadReceived);
    }

    [Fact]
    public async Task ReceiveAsync_Should_Ignore_Message_With_Wrong_Topic()
    {
        string? payloadReceived = null;

        var subscriber = CreateSubscriber(
            out _,
            out _,
            out _,
            handler: p =>
            {
                payloadReceived = p;
                return Task.CompletedTask;
            });

        var msg = Encoding.UTF8.GetBytes("wrongTopic:Hello");

        await subscriber.ReceiveAsync(msg);

        Assert.Null(payloadReceived);
    }


    [Fact]
    public async Task CreateConnection_Should_Retry_On_Failure_And_Succeed()
    {
        var subscriber = CreateSubscriber(out var connMock, out _, out _);

        int attempt = 0;

        connMock.Setup(c => c.ConnectAsync())
            .Returns(() =>
            {
                attempt++;
                if (attempt < 2)
                    throw new SubscriberConnectionException("fail", null);

                return Task.CompletedTask;
            });

        await ((ISubscriber)subscriber).CreateConnection();

        Assert.Equal(2, attempt);
    }

    [Fact]
    public async Task CreateConnection_Should_Throw_When_Max_Retries_Exceeded()
    {
        var subscriber = CreateSubscriber(out var connMock, out _, out _);

        connMock.Setup(c => c.ConnectAsync())
            .Throws(new SubscriberConnectionException("fail", null));

        await Assert.ThrowsAsync<SubscriberConnectionException>(() =>
            ((ISubscriber)subscriber).CreateConnection());
    }


    [Fact]
    public async Task StartMessageProcessingAsync_Should_Process_Incoming_Messages()
    {
        string? received = null;

        var subscriber = CreateSubscriber(
            out _,
            out var respondChannel,
            out _,
            handler: s =>
            {
                received = s;
                return Task.CompletedTask;
            });

        // Push message into channel BEFORE starting processing
        await respondChannel.Writer.WriteAsync(Encoding.UTF8.GetBytes("test:Message"));

        var task = subscriber.StartMessageProcessingAsync();

        // Give processing a moment
        await Task.Delay(50);

        subscriber.DisposeAsync().GetAwaiter().GetResult(); // stop loop

        Assert.Equal("Message", received);
    }


    [Fact]
    public async Task SendRequestAsync_Should_Write_To_RequestChannel()
    {
        var subscriber = CreateSubscriber(
            out _,
            out _,
            out var requestChannel);

        byte[] message = Encoding.UTF8.GetBytes("abc");

        await subscriber.SendRequestAsync(message);

        var read = await requestChannel.Reader.ReadAsync();

        Assert.Equal(message, read);
    }

    [Fact]
    public async Task DisposeAsync_Should_Close_Channels_And_Disconnect()
    {
        var subscriber = CreateSubscriber(
            out var connMock,
            out var respondChannel,
            out var requestChannel);

        await subscriber.DisposeAsync();

        Assert.True(requestChannel.Reader.Completion.IsCompleted);
        Assert.True(respondChannel.Reader.Completion.IsCompleted);

        connMock.Verify(c => c.DisconnectAsync(), Times.Once);
    }
}

