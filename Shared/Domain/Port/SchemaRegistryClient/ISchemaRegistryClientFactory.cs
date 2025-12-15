namespace Shared.Domain.Port.SchemaRegistryClient;

public interface ISchemaRegistryClientFactory
{
    ISchemaRegistryClient Create();
}

