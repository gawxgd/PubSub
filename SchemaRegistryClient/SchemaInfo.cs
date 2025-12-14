namespace SchemaRegistryClient;

public sealed record SchemaInfo
{
    public int SchemaId { get; set; }
    public string Json { get; }
    public int Version { get; }

    public SchemaInfo(int id, string json, int version)
    {
        SchemaId = id;
        if (json == null)
            throw new ArgumentNullException(nameof(json));
        Json = json; 
        Version = version;
    }
}