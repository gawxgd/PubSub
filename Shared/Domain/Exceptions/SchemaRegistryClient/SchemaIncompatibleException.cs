namespace Shared.Domain.Exceptions.SchemaRegistryClient;

public sealed class SchemaIncompatibleException : Exception
{
    public SchemaIncompatibleException(string topic, string? registryMessage = null, Exception? innerException = null)
        : base(BuildMessage(topic, registryMessage), innerException)
    {
        Topic = topic;
        RegistryMessage = registryMessage;
    }

    public string Topic { get; }

    public string? RegistryMessage { get; }

    private static string BuildMessage(string topic, string? registryMessage)
    {
        if (string.IsNullOrWhiteSpace(registryMessage))
        {
            return $"Schema is incompatible with the latest schema for topic '{topic}'.";
        }

        return $"Schema is incompatible with the latest schema for topic '{topic}'. Registry response: {registryMessage}";
    }
}

