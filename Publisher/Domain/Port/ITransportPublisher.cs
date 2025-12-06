namespace Publisher.Domain.Port;

public interface ITransportPublisher
{
    Task CreateConnection();
    Task PublishAsync(byte[] message);
}