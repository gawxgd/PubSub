using Subscriber.Domain;

namespace Subscriber.Outbound.Adapter;

public class TcpSubscriberConnection : ISubscriber
{
    public Task CreateConnection()
    {
        throw new NotImplementedException();
    }

    public Task ReceiveAsync(byte[] message)
    {
        throw new NotImplementedException();
    }
}