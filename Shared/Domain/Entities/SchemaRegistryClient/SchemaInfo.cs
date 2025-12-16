using System.Text.Json;

namespace Shared.Domain.Entities.SchemaRegistryClient;

public sealed record SchemaInfo(int SchemaId, JsonElement SchemaJson, int Version);