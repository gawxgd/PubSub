using System.Net;

namespace Shared.Domain.Exceptions.SchemaRegistryClient;

public sealed class SchemaRegistryException : Exception
{
    public string Endpoint { get; }
    public HttpStatusCode? StatusCode { get; }

    public SchemaRegistryException(string endpoint, HttpStatusCode statusCode, Exception? innerException = null)
        : base($"Schema registry request failed for endpoint '{endpoint}' with status code: {statusCode}", innerException)
    {
        Endpoint = endpoint;
        StatusCode = statusCode;
    }

    public SchemaRegistryException(string endpoint, Exception innerException)
        : base($"Schema registry request failed for endpoint '{endpoint}'", innerException)
    {
        Endpoint = endpoint;
    }
}

