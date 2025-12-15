namespace Publisher.Domain.Port;

public interface IMessagePublisher<T>
{
    Task PublishAsync(T message);
}