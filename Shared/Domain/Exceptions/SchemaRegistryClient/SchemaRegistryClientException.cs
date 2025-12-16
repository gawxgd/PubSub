namespace Shared.Domain.Exceptions.SchemaRegistryClient;

public sealed class SchemaRegistryClientException : Exception
{
    public string Endpoint { get; }

    public SchemaRegistryClientException(string endpoint, string message)
        : base($"Schema registry error for '{endpoint}': {message}")
    {
        Endpoint = endpoint;
    }

    public SchemaRegistryClientException(string endpoint, Exception innerException)
        : base($"Schema registry error for '{endpoint}'", innerException)
    {
        Endpoint = endpoint;
    }
}