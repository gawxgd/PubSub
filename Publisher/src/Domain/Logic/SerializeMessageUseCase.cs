using Publisher.Domain.Port;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Publisher.Domain.Logic;

public class SerializeMessageUseCase<T>(
    IAvroSerializer<T> serializer,
    ISchemaRegistryClient schemaRegistryClient,
    string topic)
{
    public async Task<byte[]> Serialize(T message)
    {
        var schema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic);
        return serializer.Serialize(message, schema);
    }
}
