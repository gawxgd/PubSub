using System.Net.Http.Json;

namespace BddE2eTests.Configuration;

public sealed class SchemaRegistryAdminClient
{
    private readonly HttpClient _http;

    public SchemaRegistryAdminClient(string host, int port, TimeSpan timeout)
    {
        _http = new HttpClient
        {
            BaseAddress = new Uri($"http://{host}:{port}"),
            Timeout = timeout
        };
    }

    public async Task RegisterSchemaAsync(string topic, string schemaJson)
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
    }
}
