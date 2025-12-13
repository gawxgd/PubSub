using System.Text.Json;

namespace Publisher.Inbound.DTOs;

public class SchemaDto
{
    public int Id { get; set; }
    public string? Topic { get; set; }
    public int Version { get; set; }

    public JsonElement SchemaJson { get; set; }
}