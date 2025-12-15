using System.Collections.Concurrent;
using System.Text.Json;
using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Exceptions.SchemaRegistryClient;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Shared.Outbound.SchemaRegistryClient;

/// <summary>
/// HTTP-based client for the Schema Registry service
/// </summary>
public sealed class HttpSchemaRegistryClient : ISchemaRegistryClient, IDisposable
{
    private readonly HttpClient _http;
    private readonly SchemaRegistryClientOptions _options;
    private readonly bool _ownsHttpClient;
    private readonly ConcurrentDictionary<int, CachedSchema> _cacheById = new();
    private readonly ConcurrentDictionary<string, CachedSchema> _cacheByTopic = new();

    /// <summary>
    /// Creates a new SchemaRegistryClient with a new HttpClient (client will be disposed)
    /// </summary>
    public HttpSchemaRegistryClient(SchemaRegistryClientOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _http = new HttpClient
        {
            BaseAddress = options.BaseAddress,
            Timeout = options.Timeout
        };
        _ownsHttpClient = true;
    }

    /// <summary>
    /// Creates a new SchemaRegistryClient with an existing HttpClient (client will NOT be disposed)
    /// Use this when using IHttpClientFactory
    /// </summary>
    public HttpSchemaRegistryClient(HttpClient httpClient, SchemaRegistryClientOptions options)
    {
        _http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _http.BaseAddress = options.BaseAddress;
        _http.Timeout = options.Timeout;
        _ownsHttpClient = false;
    }

    public async Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default)
    {
        // Check cache
        if (_cacheById.TryGetValue(schemaId, out var cached))
        {
            if (_options.CacheExpiration.HasValue && cached.IsExpired(_options.CacheExpiration.Value))
            {
                _cacheById.TryRemove(schemaId, out _);
            }
            else
            {
                return cached.Schema;
            }
        }

        // Fetch from registry
        var response = await _http.GetAsync($"schema/id/{schemaId}", cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            return HandleNotFound<SchemaInfo?>(() => new SchemaNotFoundException(schemaId));
        }

        var schema = await DeserializeSchemaResponse(response, cancellationToken);

        if (schema == null)
        {
            return HandleNotFound<SchemaInfo?>(() => new SchemaNotFoundException(schemaId));
        }

        // Cache it TODO retrieve topic and cache in the other cache too (dto contains it)
        _cacheById[schemaId] = new CachedSchema(schema);

        return schema;
    }

    public async Task<SchemaInfo?> GetLatestSchemaByTopicAsync(string topic,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topic))
            throw new ArgumentException("Topic cannot be null or empty", nameof(topic));

        // Check cache
        if (_cacheByTopic.TryGetValue(topic, out var cached))
        {
            if (_options.CacheExpiration.HasValue && cached.IsExpired(_options.CacheExpiration.Value))
            {
                _cacheByTopic.TryRemove(topic, out _);
                _cacheById.TryRemove(cached.Schema.SchemaId, out _);
            }
            else
            {
                return cached.Schema;
            }
        }

        // Fetch from registry
        var response = await _http.GetAsync($"schema/topic/{topic}", cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            return HandleNotFound<SchemaInfo?>(() => new SchemaNotFoundException(topic));
        }

        var schema = await DeserializeSchemaResponse(response, cancellationToken);

        if (schema == null)
        {
            return HandleNotFound<SchemaInfo?>(() => new SchemaNotFoundException(topic));
        }

        // Cache it in both caches
        var cachedSchema = new CachedSchema(schema);
        _cacheByTopic[topic] = cachedSchema;
        _cacheById[schema.SchemaId] = cachedSchema;

        return schema;
    }

    private async Task<SchemaInfo?> DeserializeSchemaResponse(HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        var content = await response.Content.ReadAsStringAsync(cancellationToken);

        var dto = JsonSerializer.Deserialize<SchemaDto>(
            content,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (dto == null)
            return null;

        return new SchemaInfo(dto.Id, dto.SchemaJson.GetRawText(), dto.Version);
    }

    private T HandleNotFound<T>(Func<Exception> exceptionFactory)
    {
        if (_options.NotFoundBehavior == NotFoundBehavior.ThrowException)
        {
            throw exceptionFactory();
        }

        return default(T)!;
    }

    public void Dispose()
    {
        if (_ownsHttpClient)
        {
            _http?.Dispose();
        }
    }
}