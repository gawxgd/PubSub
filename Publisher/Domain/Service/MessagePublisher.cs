using Publisher.Domain.Port;
using Publisher.Outbound.Adapter;

namespace Publisher.Domain.Service;

public class MessagePublisher : IMessagePublisher
{
    private readonly ITransportPublisher _transportPublisher;   
    private readonly IAvroSerializer _serializer;
    private readonly ISchemaRegistryClient _schemaRegistryClient;

    public async Task PublishAsync<T>(T message)
    {
        // fetch the avro schema from the schema registry using the _schemaRegistryClient
        
        // serialize using IAvroSerializer - this returns a byte payload - message wrapped like: topic-message-separator
        
        // use the Transport Publisher to actually send the byte payload
    }
}
