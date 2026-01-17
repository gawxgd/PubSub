using System.Threading.Channels;
using BddE2eTests.Configuration.TestEvents;

namespace BddE2eTests.Configuration;

public sealed class SubscriberHandle(object subscriber, Type messageType, Channel<ITestEvent> receivedMessages)
{
    public object Subscriber { get; } = subscriber ?? throw new ArgumentNullException(nameof(subscriber));
    public Type MessageType { get; } = messageType ?? throw new ArgumentNullException(nameof(messageType));
    public Channel<ITestEvent> ReceivedMessages { get; } =
        receivedMessages ?? throw new ArgumentNullException(nameof(receivedMessages));

    public Task StartConnectionAsync(ulong? initialOffset = null)
    {
        return ((dynamic)Subscriber).StartConnectionAsync(initialOffset);
    }

    public Task StartMessageProcessingAsync()
    {
        return ((dynamic)Subscriber).StartMessageProcessingAsync();
    }

    public ulong? GetCommittedOffset()
    {
        return ((dynamic)Subscriber).GetCommittedOffset();
    }

    public async ValueTask DisposeAsync()
    {
        if (Subscriber is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else if (Subscriber is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}

