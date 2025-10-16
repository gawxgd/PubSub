using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Infrastructure.Logging;

internal sealed class HttpLogger : ILogger
{
	private readonly Channel<HttpLogEvent> _channel;
	private readonly string _source;
	private readonly string _category;

	public HttpLogger(Channel<HttpLogEvent> channel, string source, string category)
	{
		_channel = channel;
		_source = source;
		_category = category;
	}

	public IDisposable? BeginScope<TState>(TState state) where TState : notnull => default;

	public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

	public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
	{
		if (!IsEnabled(logLevel)) return;
		var message = formatter(state, exception);
		var context = new Dictionary<string, object?>
		{
			{"category", _category},
			{"eventId", eventId.Id},
			{"name", eventId.Name},
			{"exception", exception?.ToString()}
		};
		_channel.Writer.TryWrite(new HttpLogEvent(DateTimeOffset.UtcNow, _source, logLevel.ToString(), message, context));
	}
}

