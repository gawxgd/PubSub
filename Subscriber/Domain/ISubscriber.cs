namespace Subscriber.Domain;

public interface ISubscriber
{ 
        Task CreateConnection(); 
        Task ReceiveAsync(byte[] message);
}