using System.Threading.Channels;
using MessageBroker.Domain.Entities;

namespace Publisher.Domain.Port;

public interface IPublisherConnection
{
    Task ConnectAsync();
    Task DisconnectAsync();
    ChannelReader<PublishResponse> Responses { get; }
}