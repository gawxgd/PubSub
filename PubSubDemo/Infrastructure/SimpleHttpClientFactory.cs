using System.Net.Http;

namespace PubSubDemo.Infrastructure;

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

