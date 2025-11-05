namespace Subscriber.Domain;

public interface ISubscriberConnection
{
    Task ConnectAsync();
    Task DisconnectAsync();
}
