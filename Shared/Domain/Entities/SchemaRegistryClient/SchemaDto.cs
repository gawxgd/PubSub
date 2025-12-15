using System.Text.Json;

namespace Shared.Domain.Entities.SchemaRegistryClient;

public class SchemaDto
{
    public int Id { get; set; }
    public string? Topic { get; set; }
    public int Version { get; set; }

    public JsonElement SchemaJson { get; set; }
}