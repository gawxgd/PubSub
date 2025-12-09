using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace Subscriber.Outbound.Adapter;

public class MessageValidator(string topic, int minMessageLength, int maxMessageLength)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageValidator>(LogSource.MessageBroker);

    public ValidationResult Validate(byte[] message)
    {
        var text = System.Text.Encoding.UTF8.GetString(message);

        if (text.Length < minMessageLength || text.Length > maxMessageLength)
        {
            Logger.LogError($"Invalid message length: {text.Length}");
            return ValidationResult.Invalid($"Invalid message length: {text.Length}");
        }

        if (!text.StartsWith($"{topic}:"))
        {
            Logger.LogError($"Ignored message (wrong topic): {text}");
            return ValidationResult.Invalid($"Wrong topic: expected {topic}");
        }

        var payload = text.Substring(topic.Length + 1);
        return ValidationResult.Valid(payload);
    }
}

public record ValidationResult(bool IsValid, string? Payload, string? ErrorMessage)
{
    public static ValidationResult Valid(string payload) => new(true, payload, null);
    public static ValidationResult Invalid(string error) => new(false, null, error);
}

