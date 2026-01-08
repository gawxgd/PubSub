using System.Text.Json;

namespace Shared.Domain.Entities.SchemaRegistryClient;

using System.Text.Json;
using System.Text.Json.Serialization;

public sealed record SchemaInfo(
    [property: JsonPropertyName("id")] int SchemaId,
    JsonElement SchemaJson,
    int Version
);
