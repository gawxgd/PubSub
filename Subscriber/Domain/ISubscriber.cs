namespace Subscriber.Domain;

public interface ISubscriber
{
        Task CreateConnection(CancellationToken cancellationToken);
        Task ReceiveAsync(byte[] message, CancellationToken cancellationToken);
        Task StartAsync(CancellationToken cancellationToken);
}
