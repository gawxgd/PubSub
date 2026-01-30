namespace SchemaRegistry;

using SchemaRegistry.Domain.Enums;

public record SchemaRegistryOptions
{
    public string StorageType { get; init; } = "File";
    public CompatibilityMode CompatibilityMode { get; set; } = CompatibilityMode.Full;
    public string? FileStoreFolderPath { get; init; } = "data/schemas";
    public string? ConnectionString { get; init; }
}
