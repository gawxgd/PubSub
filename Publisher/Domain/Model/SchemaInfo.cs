namespace Publisher.Domain.Model;

public sealed class SchemaInfo
{
    public string Json { get; }
    public int Version { get; }

    public SchemaInfo(string json, int version)
    {
        Json = json ?? throw new ArgumentNullException(nameof(json));
        Version = version;
    }
}