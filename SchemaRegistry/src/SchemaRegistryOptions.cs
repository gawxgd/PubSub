namespace SchemaRegistry;

public record SchemaRegistryOptions
{
    public string StorageType { get; init; } = "File";
    public string CompatibilityMode { get; init; } = "FULL";
    public string? FileStoreFolderPath { get; init; } = "data/schemas";
    public string? ConnectionString { get; init; }
}