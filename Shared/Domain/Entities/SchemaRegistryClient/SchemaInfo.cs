namespace Shared.Domain.Entities.SchemaRegistryClient;

public sealed record SchemaInfo(int SchemaId, string Json, int Version);