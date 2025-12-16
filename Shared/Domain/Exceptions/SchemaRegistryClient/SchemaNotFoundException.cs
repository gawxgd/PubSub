namespace Shared.Domain.Exceptions.SchemaRegistryClient;

public sealed class SchemaNotFoundException : Exception
{
    public SchemaNotFoundException(string topic)
        : base($"Schema not found for topic: {topic}")
    {
    }

    public SchemaNotFoundException(int schemaId)
        : base($"Schema not found for ID: {schemaId}")
    {
    }
}