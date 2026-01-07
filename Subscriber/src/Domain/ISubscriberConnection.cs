using System.Threading;

namespace Subscriber.Domain;

public interface ISubscriberConnection
{
    Task ConnectAsync();
    Task DisconnectAsync();
    //TODO consider place to put this metod as required?
    //Task SendRequestAsync(string topic, ulong offset, CancellationToken cancellationToken = default);
}
