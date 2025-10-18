namespace Publisher.Domain.Port;

public interface IPublisher
{
    Task ConnectAsync(CancellationToken cancellationToken);

    Task PublishAsync(byte[] message, CancellationToken cancellationToken = default); // ToDo change args
}