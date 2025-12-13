using Publisher.Configuration.Options;

namespace Publisher.Domain.Port;

public interface IPublisherFactory
{
    ITransportPublisher CreatePublisher(PublisherOptions options);
}