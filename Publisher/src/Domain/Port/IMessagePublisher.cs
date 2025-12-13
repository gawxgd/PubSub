namespace Publisher.Domain.Port;

public interface IMessagePublisher
{
    Task PublishAsync<T>(T message);
}