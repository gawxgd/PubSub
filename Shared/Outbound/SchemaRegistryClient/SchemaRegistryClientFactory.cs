using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Shared.Outbound.SchemaRegistryClient;

public sealed class SchemaRegistryClientFactory(
    IHttpClientFactory httpClientFactory,
    SchemaRegistryClientOptions options)
    : ISchemaRegistryClientFactory
{
    public ISchemaRegistryClient Create()
    {
        var httpClient = httpClientFactory.CreateClient("SchemaRegistry");
        return new HttpSchemaRegistryClient(httpClient, options);
    }
}

