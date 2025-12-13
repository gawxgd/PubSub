using Publisher.Domain.Port;

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
        if (message == null)
            throw new ArgumentNullException(nameof(message));
        
        // fetch the avro schema from the schema registry using the _schemaRegistryClient
        var schema = await schemaRegistryClient.GetSchemaAsync(topic);
        
        // serialize using IAvroSerializer - this returns a byte payload - [schemaId][message]
        var payload = await serializer.SerializeAsync(message, schema);
        
        // TODO: check if _transportPublisher is already connected?
        
        // use the Transport Publisher to actually send the byte payload
        await transportPublisher.PublishAsync(payload);
    }
}
