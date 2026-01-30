using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace BddE2eTests.Configuration;

public sealed class SchemaRegistryAdminClient
{
    private readonly HttpClient _http;

    public SchemaRegistryAdminClient(string host, int port, TimeSpan timeout)
    {
        _http = new HttpClient
        {
            BaseAddress = new Uri($"http:
            Timeout = timeout
        };
    }

    private sealed record RegisterSchemaResponse(int Id);

    public async Task<int> RegisterSchemaAsync(string topic, string schemaJson)
    {
        var response = await _http.PostAsJsonAsync(
            $"/schema/topic/{topic}",
            new { schema = schemaJson });

        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"Failed to register schema for topic '{topic}'. " +
                $"Status={response.StatusCode}, Body={body}");
        }

        var dto = await response.Content.ReadFromJsonAsync<RegisterSchemaResponse>();
        if (dto == null || dto.Id <= 0)
        {
            var body = await response.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"Schema registry returned unexpected register response for topic '{topic}'. Body={body}");
        }

        return dto.Id;
    }

    public sealed record LatestSchemaResponse(
        int Id,
        int Version,
        JsonElement SchemaJson);

    public async Task<LatestSchemaResponse?> GetLatestSchemaAsync(string topic)
    {
        var response = await _http.GetAsync($"/schema/topic/{topic}");

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"Failed to get latest schema for topic '{topic}'. " +
                $"Status={response.StatusCode}, Body={body}");
        }

        var dto = await response.Content.ReadFromJsonAsync<LatestSchemaResponse>();
        return dto;
    }

    public sealed record SchemaByIdResponse(
        int Id,
        string Topic,
        int Version,
        JsonElement SchemaJson);

    public async Task<SchemaByIdResponse?> GetSchemaByIdAsync(int id)
    {
        var response = await _http.GetAsync($"/schema/id/{id}");

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"Failed to get schema by id '{id}'. " +
                $"Status={response.StatusCode}, Body={body}");
        }

        var dto = await response.Content.ReadFromJsonAsync<SchemaByIdResponse>();
        return dto;
    }
}
