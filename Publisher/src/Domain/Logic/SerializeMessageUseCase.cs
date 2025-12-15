using Publisher.Domain.Port;
using SchemaRegistryClient;
using SchemaRegistryClient.Domain.Port.SchemaRegistry;

namespace Publisher.Domain.Logic;

public class SerializeMessageUseCase<T>(
    IAvroSerializer<T> serializer,
    ISchemaRegistryClient schemaRegistryClient,
    string topic)
{
    public async Task<byte[]> Serialize(T message)
    {
        var schema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic);

        return await serializer.SerializeAsync(message, schema);
    }
}