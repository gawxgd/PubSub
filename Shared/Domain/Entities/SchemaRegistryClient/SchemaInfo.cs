using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shared.Domain.Entities.SchemaRegistryClient;

public sealed record SchemaInfo(
    [property: JsonPropertyName("id")] int SchemaId,
    JsonElement SchemaJson,
    int Version);