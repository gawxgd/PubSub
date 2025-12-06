using Publisher.Domain.Port;
using Publisher.Outbound.Adapter;

namespace Publisher.Domain.Service;

public class MessagePublisher(
    ITransportPublisher transportPublisher,
    IAvroSerializer serializer,
    ISchemaRegistryClient schemaRegistryClient,
    string topic)
    : IMessagePublisher
{

    public async Task PublishAsync<T>(T message)
    {
        // fetch the avro schema from the schema registry using the _schemaRegistryClient
        var schema = await schemaRegistryClient.GetSchemaAsync(topic);
        
        // serialize using IAvroSerializer - this returns a byte payload - message wrapped like: topic-message-separator
        var payload = await serializer.SerializeAsync(message, schema, topic);
        
        // TODO: check if _transportPublisher is already connected?
        
        // use the Transport Publisher to actually send the byte payload
        await transportPublisher.PublishAsync(payload);
    }
}
