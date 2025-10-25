namespace Publisher.Domain.Port;

public interface IPublisherConnection
{
    Task ConnectAsync();
    Task DisconnectAsync();
}