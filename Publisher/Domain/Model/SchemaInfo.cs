namespace Publisher.Domain.Model;

public sealed record SchemaInfo
{
    public string Json { get; }
    public int Version { get; }

    public SchemaInfo(string json, int version)
    {
        if (json == null)
            throw new ArgumentNullException(nameof(json));
        Json = json; 
        Version = version;
    }
}