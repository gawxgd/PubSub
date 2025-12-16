namespace Subscriber.Domain;

public interface ISubscriber<T> where T : new()
{
    internal Task CreateConnection();
    Task ReceiveAsync(byte[] message);
    Task StartMessageProcessingAsync();
    Task StartConnectionAsync();
}
