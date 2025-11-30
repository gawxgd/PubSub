using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace SchemaRegistry.Inbound.DTOs;

// TODO: is this thing worth creating a class? 

public record RegisterRequest
{
    [Required]
    [JsonPropertyName("Schema")]
    public string Schema { get; set; } = null!;
}