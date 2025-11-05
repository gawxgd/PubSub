namespace Publisher.Domain.Port;

public interface IPublisher
{
    Task CreateConnection();
    Task PublishAsync(byte[] message);
}