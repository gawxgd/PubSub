using BddE2eTests.Configuration.Options;
using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Domain.Port.SchemaRegistryClient;
using Shared.Outbound.SchemaRegistryClient;

namespace BddE2eTests.Configuration.Builder;

public class SchemaRegistryClientBuilder(SchemaRegistryTestOptions options)
{
    private string _host = options.Host;
    private int _port = options.Port;
    private TimeSpan _timeout = TimeSpan.FromSeconds(options.TimeoutSeconds);

    public SchemaRegistryClientBuilder WithHost(string host)
    {
        _host = host ?? throw new ArgumentNullException(nameof(host));
        return this;
    }

    public SchemaRegistryClientBuilder WithPort(int port)
    {
        if (port <= 0) throw new ArgumentException("Port must be positive", nameof(port));
        _port = port;
        return this;
    }

    public SchemaRegistryClientBuilder WithTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero) throw new ArgumentException("Timeout must be positive", nameof(timeout));
        _timeout = timeout;
        return this;
    }

    public Uri BuildUri() => new Uri($"http://{_host}:{_port}");
    
    public ISchemaRegistryClient Build()
    {
        var uri = BuildUri();
        var clientOptions = new SchemaRegistryClientOptions(uri, _timeout);
        var httpClientFactory = new TestHttpClientFactory(uri);
        var factory = new SchemaRegistryClientFactory(httpClientFactory, clientOptions);
        return factory.Create();
    }

    public ISchemaRegistryClientFactory BuildFactory()
    {
        var uri = BuildUri();
        var clientOptions = new SchemaRegistryClientOptions(uri, _timeout);
        var httpClientFactory = new TestHttpClientFactory(uri);
        return new SchemaRegistryClientFactory(httpClientFactory, clientOptions);
    }

    private class TestHttpClientFactory(Uri baseAddress) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name)
        {
            return new HttpClient
            {
                BaseAddress = baseAddress
            };
        }
    }
}

