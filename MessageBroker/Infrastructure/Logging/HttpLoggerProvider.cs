using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Infrastructure.Logging;

public sealed class HttpLoggerProvider : ILoggerProvider
{
	private readonly Channel<HttpLogEvent> _channel;
	private readonly string _source;

	public HttpLoggerProvider(Channel<HttpLogEvent> channel)
	{
		_channel = channel;
		_source = "MessageBroker";
	}

	public ILogger CreateLogger(string categoryName) => new HttpLogger(_channel, _source, categoryName);

	public void Dispose() { }
}


