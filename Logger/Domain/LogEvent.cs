namespace Logger.Domain;

public record LogEvent(
	DateTimeOffset Timestamp,
	string Source,
	string Level,
	string Message,
	Dictionary<string, object?>? Context
);


