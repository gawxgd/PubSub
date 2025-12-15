using System.Net;
using System.Text.Json;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Exceptions.SchemaRegistryClient;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Shared.Outbound.SchemaRegistryClient;

public sealed class HttpSchemaRegistryClient : ISchemaRegistryClient
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<HttpSchemaRegistryClient>(LogSource.Other);

    private readonly HttpClient _httpClient;
    private readonly ISchemaCache _cache;

    public HttpSchemaRegistryClient(HttpClient httpClient, SchemaRegistryClientOptions options)
    {
        _httpClient = httpClient;
        _httpClient.BaseAddress = options.BaseAddress;
        _httpClient.Timeout = options.Timeout;
        _cache = new InMemorySchemaCache(options.CacheExpiration);
    }

    public async Task<SchemaInfo> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default)
    {
        if (_cache.TryGet(schemaId, out var cached))
        {
            Logger.LogDebug($"Cache hit for schema ID: {schemaId}");
            return cached;
        }

        Logger.LogDebug($"Cache miss for schema ID: {schemaId}, fetching from registry");
        var schema = await FetchSchemaAsync($"schema/id/{schemaId}", cancellationToken);

        _cache.AddToCache(schemaId, schema);
        return schema;
    }

    public async Task<SchemaInfo?> GetLatestSchemaByTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        if (_cache.TryGet(topic, out var cached))
        {
            Logger.LogDebug($"Cache hit for topic: {topic}");
            return cached;
        }

        Logger.LogDebug($"Cache miss for topic: {topic}, fetching from registry");
        var schema = await FetchSchemaAsync($"schema/topic/{topic}", cancellationToken);

        _cache.AddToCache(topic, schema);
        return schema;
    }

    private async Task<SchemaInfo> FetchSchemaAsync(string endpoint, CancellationToken cancellationToken)
    {
        HttpResponseMessage response;

        try
        {
            response = await _httpClient.GetAsync(endpoint, cancellationToken);
        }
        catch (HttpRequestException ex)
        {
            Logger.LogError($"HTTP request failed for endpoint: {endpoint}", ex);
            throw new SchemaRegistryException(endpoint, ex);
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            Logger.LogError($"Request timeout for endpoint: {endpoint}", ex);
            throw new SchemaRegistryException(endpoint, ex);
        }

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            Logger.LogWarning($"Schema not found at endpoint: {endpoint}");
            throw new SchemaNotFoundException(endpoint);
        }

        if (!response.IsSuccessStatusCode)
        {
            Logger.LogError($"Unexpected status code {response.StatusCode} for endpoint: {endpoint}");
            throw new SchemaRegistryException(endpoint, response.StatusCode);
        }

        try
        {
            var content = await response.Content.ReadAsStringAsync(cancellationToken);
            var dto = JsonSerializer.Deserialize<SchemaDto>(content, JsonOptions);

            if (dto is null)
            {
                Logger.LogError($"Failed to deserialize schema response from endpoint: {endpoint}");
                throw new SchemaDeserializationException(endpoint);
            }

            Logger.LogDebug($"Successfully fetched schema from endpoint: {endpoint}");
            return new SchemaInfo(dto.Id, dto.SchemaJson.GetRawText(), dto.Version);
        }
        catch (JsonException ex)
        {
            Logger.LogError($"JSON deserialization failed for endpoint: {endpoint}", ex);
            throw new SchemaDeserializationException(endpoint, ex);
        }
    }
}
