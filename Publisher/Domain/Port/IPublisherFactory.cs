using Publisher.Configuration.Options;

namespace Publisher.Domain.Port;

public interface IPublisherFactory
{
    IPublisher CreatePublisher(PublisherOptions options);
}