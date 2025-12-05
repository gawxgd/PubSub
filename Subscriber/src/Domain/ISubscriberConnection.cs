using System.Threading;

namespace Subscriber.Domain;

public interface ISubscriberConnection
{
    Task ConnectAsync();
    Task DisconnectAsync();
    //Task SendRequestAsync(string topic, ulong offset, CancellationToken cancellationToken = default);
}
