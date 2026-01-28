namespace Shared.Domain.Avro;

[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class AvroRecordNameAttribute(string name) : Attribute
{
    public string Name { get; } = string.IsNullOrWhiteSpace(name)
        ? throw new ArgumentException("Name must be provided", nameof(name))
        : name;

    public string? Namespace { get; init; }
}

