using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace SchemaRegistry.Inbound.DTOs;

public record RegisterRequest
{
    [Required]
    [JsonPropertyName("Schema")]
    public string Schema { get; set; } = null!;
}
