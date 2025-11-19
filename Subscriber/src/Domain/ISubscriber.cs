namespace Subscriber.Domain;

public interface ISubscriber
{
        internal Task CreateConnection();
        Task ReceiveAsync(byte[] message);
        Task StartMessageProcessingAsync();
        Task StartConnectionAsync();
        Task SendRequestAsync(byte[] message);
}
