using Publisher.Domain.Port;
using SchemaRegistryClient;

namespace Publisher.Domain.Service;

public class MessagePublisher<T>(
    ITransportPublisher transportPublisher,
    IAvroSerializer<T> serializer,
    ISchemaRegistryClient schemaRegistryClient,
    string topic)
    : IMessagePublisher<T>
{

    public async Task PublishAsync(T message)
    {
        if (message == null)
            throw new ArgumentNullException(nameof(message));
        
        // fetch the avro schema from the schema registry using the _schemaRegistryClient
        var schema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic);
        
        // serialize using IAvroSerializer - this returns a byte payload - [schemaId][message]
        var payload = await serializer.SerializeAsync(message, schema);
        
        // TODO: check if _transportPublisher is already connected?
        
        // use the Transport Publisher to actually send the byte payload
        await transportPublisher.PublishAsync(payload);
    }
}
