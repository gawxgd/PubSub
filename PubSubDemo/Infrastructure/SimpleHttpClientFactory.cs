using System.Net.Http;

namespace PubSubDemo.Infrastructure;

/// <summary>
/// Simple implementation of IHttpClientFactory for demo purposes.
/// </summary>
public sealed class SimpleHttpClientFactory : IHttpClientFactory, IDisposable
{
    private readonly HttpClient _httpClient;

    public SimpleHttpClientFactory()
    {
        _httpClient = new HttpClient();
    }

    public HttpClient CreateClient(string name)
    {
        return _httpClient;
    }

    public void Dispose()
    {
        _httpClient?.Dispose();
    }
}

