using System.Net.Http.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;

namespace MessageBroker.Infrastructure.Logging;

public sealed class HttpLogDispatcher : BackgroundService
{
	public const string HttpClientName = "LoggerForwarder";
	private readonly Channel<HttpLogEvent> _channel;
	private readonly IHttpClientFactory _clientFactory;
	private readonly LoggerOptions _options;

	public HttpLogDispatcher(Channel<HttpLogEvent> channel, IHttpClientFactory clientFactory, Microsoft.Extensions.Options.IOptions<LoggerOptions> options)
	{
		_channel = channel;
		_clientFactory = clientFactory;
		_options = options.Value;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		if (string.IsNullOrWhiteSpace(_options.BaseUrl))
		{
			return;
		}
		var client = _clientFactory.CreateClient(HttpClientName);
		client.BaseAddress = new Uri(_options.BaseUrl, UriKind.Absolute);
		await foreach (var evt in _channel.Reader.ReadAllAsync(stoppingToken))
		{
			try
			{
				await client.PostAsJsonAsync("/ingest", evt, cancellationToken: stoppingToken);
			}
			catch
			{
				// best-effort forwarding; avoid crashing logging
			}
		}
	}
}


