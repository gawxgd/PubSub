namespace Shared.Domain.Exceptions.SchemaRegistryClient;

public sealed class SchemaDeserializationException : Exception
{
    public string Endpoint { get; }

    public SchemaDeserializationException(string endpoint, Exception? innerException = null)
        : base($"Failed to deserialize schema response from endpoint: {endpoint}", innerException)
    {
        Endpoint = endpoint;
    }
}

