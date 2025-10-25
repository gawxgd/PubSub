namespace Subscriber.Domain;

public interface ISubscriberConnection
{
    Task ConnectAsync(CancellationToken cancellationToken = default);
    Task DisconnectAsync(CancellationToken cancellationToken = default);
}
