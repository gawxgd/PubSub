namespace Subscriber.Domain;

public interface ISubscriber<T>
{
        internal Task CreateConnection();
        Task ReceiveAsync(byte[] message);
        Task StartMessageProcessingAsync();
        Task StartConnectionAsync();
}
