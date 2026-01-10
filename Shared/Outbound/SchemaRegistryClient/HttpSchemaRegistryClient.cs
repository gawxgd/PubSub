using System.Net;
using System.Net.Http.Json;
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
    private const string IdEndpoint = "schema/id/";
    private const string TopicEndpoint = "schema/topic/";

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<HttpSchemaRegistryClient>(LogSource.Other);

    private readonly HttpClient _httpClient;
    private readonly ISchemaCache _cache;

    public HttpSchemaRegistryClient(HttpClient httpClient, SchemaRegistryClientOptions options)
    {
        _httpClient = httpClient;
        if (_httpClient.BaseAddress == null)
        {
            _httpClient.BaseAddress = options.BaseAddress;
        }
        _httpClient.Timeout = options.Timeout;
        _cache = new InMemorySchemaCache(options.CacheExpiration);
    }

    public async Task<SchemaInfo> RegisterSchemaAsync(string topic, string schemaJson,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(schemaJson);

        try
        {
            // RegisterRequest expects "Schema" with capital S (see SchemaRegistry/src/Inbound/DTOs/RegisterRequest.cs)
            var request = new { Schema = schemaJson };
            var response = await _httpClient.PostAsJsonAsync($"{TopicEndpoint}{topic}", request, cancellationToken);

            if (response.StatusCode == HttpStatusCode.Conflict)
            {
                var conflictContent = await response.Content.ReadAsStringAsync(cancellationToken);
                Logger.LogError($"Schema incompatible for topic '{topic}'. Registry response: {conflictContent}");
                throw new SchemaIncompatibleException(topic, conflictContent);
            }

            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync(cancellationToken);
            var result = JsonSerializer.Deserialize<SchemaRegistrationResponse>(content, JsonOptions)!;

            Logger.LogInfo($"Registered schema for topic '{topic}' with ID: {result.Id}");

            return await GetSchemaByIdAsync(result.Id, cancellationToken);
        }
        catch (SchemaIncompatibleException)
        {
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogError($"Failed to register schema for topic: {topic}", ex);
            throw new SchemaRegistryClientException($"Failed to register schema for topic: {topic}", ex);
        }
    }

    private record SchemaRegistrationResponse(int Id);

    public async Task<SchemaInfo> GetLatestSchemaByTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        if (_cache.TryGet(topic, out var cached))
        {
            Logger.LogDebug($"Cache hit for topic: {topic}");
            return cached;
        }

        Logger.LogDebug($"Cache miss for topic: {topic}, fetching from registry");
        var schema = await FetchSchemaAsync($"{TopicEndpoint}{topic}", cancellationToken);

        _cache.AddToCache(topic, schema);
        return schema;
    }

    public async Task<SchemaInfo> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default)
    {
        if (_cache.TryGet(schemaId, out var cached))
        {
            Logger.LogDebug($"Cache hit for schema ID: {schemaId}");
            return cached;
        }

        Logger.LogDebug($"Cache miss for schema ID: {schemaId}, fetching from registry");
        var schema = await FetchSchemaAsync($"{IdEndpoint}{schemaId}", cancellationToken);

        _cache.AddToCache(schemaId, schema);
        return schema;
    }

    private async Task<SchemaInfo> FetchSchemaAsync(string endpoint, CancellationToken cancellationToken)
    {
        try
        {
            var response = await _httpClient.GetAsync(endpoint, cancellationToken);

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                Logger.LogError($"Schema not found: {endpoint}");
                throw new SchemaNotFoundException(endpoint);
            }

            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            var schemaInfo = JsonSerializer.Deserialize<SchemaInfo>(content, JsonOptions)!;

            Logger.LogInfo($"Successfully fetched the schema from: {endpoint}");
            return schemaInfo;
        }
        catch (SchemaNotFoundException)
        {
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogError($"Failed to fetch schema from endpoint: {endpoint}", ex);
            throw new SchemaRegistryClientException(endpoint, ex);
        }
    }
}