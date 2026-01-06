using System.Threading.Channels;
using MessageBroker.Domain.Entities;
using Publisher.Domain.Exceptions;

namespace Publisher.Domain.Port;

public interface IPublisher<in T>
{
    Task CreateConnection();

    /// <exception cref="SerializationException">
    /// Thrown when message serialization fails.
    /// </exception>
    Task PublishAsync(T message);
    ChannelReader<PublishResponse>? Responses { get; }
}
