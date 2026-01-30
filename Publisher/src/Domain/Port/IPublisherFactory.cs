using Publisher.Configuration.Options;

namespace Publisher.Domain.Port;

public interface IPublisherFactory<in T>
{
    IPublisher<T> CreatePublisher(PublisherOptions options);
}
