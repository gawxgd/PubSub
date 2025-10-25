using Subscriber.Configuration;
using Subscriber.Configuration.Options;

namespace Subscriber.Domain;

public interface ISubscriberFactory
{
    ISubscriber CreateSubscriber(SubscriberOptions options, Func<string, Task>? messageHandler);
}