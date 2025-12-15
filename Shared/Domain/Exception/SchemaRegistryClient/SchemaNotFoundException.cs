namespace SchemaRegistryClient;

public sealed class SchemaNotFoundException : Exception
{
    public string? Topic { get; }
    public int? SchemaId { get; }
    
    public SchemaNotFoundException(string topic) 
        : base($"Schema not found for topic: {topic}")
    {
        Topic = topic;
    }
    
    public SchemaNotFoundException(int schemaId) 
        : base($"Schema not found for ID: {schemaId}")
    {
        SchemaId = schemaId;
    }
}