namespace Publisher.Domain.Port;

public interface IPublisher<in T>
{
    Task CreateConnection();
    Task PublishAsync(T message);
}