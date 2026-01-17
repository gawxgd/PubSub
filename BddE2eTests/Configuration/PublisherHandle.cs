using System.Threading.Channels;
using BddE2eTests.Configuration.TestEvents;
using MessageBroker.Domain.Entities;

namespace BddE2eTests.Configuration;

public sealed class PublisherHandle(object publisher, Type messageType)
{
    public object Publisher { get; } = publisher ?? throw new ArgumentNullException(nameof(publisher));
    public Type MessageType { get; } = messageType ?? throw new ArgumentNullException(nameof(messageType));

    public Task CreateConnection()
    {
        return ((dynamic)Publisher).CreateConnection();
    }

    public Task PublishAsync(ITestEvent message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!MessageType.IsInstanceOfType(message))
        {
            throw new ArgumentException(
                $"Publisher expects messages of type '{MessageType.Name}', got '{message.GetType().Name}'",
                nameof(message));
        }

        return ((dynamic)Publisher).PublishAsync((dynamic)message);
    }

    public Task<bool> WaitForAcknowledgmentsAsync(int count, TimeSpan timeout)
    {
        return ((dynamic)Publisher).WaitForAcknowledgmentsAsync(count, timeout);
    }

    public long AcknowledgedCount => ((dynamic)Publisher).AcknowledgedCount;

    public ChannelReader<PublishResponse> ErrorResponses => ((dynamic)Publisher).ErrorResponses;

    public async ValueTask DisposeAsync()
    {
        if (Publisher is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else if (Publisher is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}

