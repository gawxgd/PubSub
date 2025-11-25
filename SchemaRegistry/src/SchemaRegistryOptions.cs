namespace SchemaRegistry;

public record SchemaRegistryOptions
{
    public string CompatibilityMode { get; init; } = "FULL";
    public string FileStoreFolderPath { get; init; } = "data/schemas";
}