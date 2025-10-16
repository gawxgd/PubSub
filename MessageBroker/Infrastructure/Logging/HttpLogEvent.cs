namespace MessageBroker.Infrastructure.Logging;

public record HttpLogEvent(DateTimeOffset Timestamp, string Source, string Level, string Message, Dictionary<string, object?>? Context);


