using System.Net.Http;

namespace PerformanceTests.Infrastructure;

/// <summary>
/// Simple HTTP client factory for performance tests.
/// </summary>
public sealed class SimpleHttpClientFactory : IHttpClientFactory, IDisposable
{
    private readonly HttpClient _httpClient;

    public SimpleHttpClientFactory()
    {
        _httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(30)
        };
    }

    public HttpClient CreateClient(string? name = null)
    {
        return _httpClient;
    }

    public void Dispose()
    {
        _httpClient?.Dispose();
    }
}

