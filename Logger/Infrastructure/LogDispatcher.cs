using System.Threading.Channels;
using Logger.Domain;
using Logger.Realtime;
using Microsoft.AspNetCore.SignalR;

namespace Logger.Infrastructure;

public class LogDispatcher : BackgroundService
{
	private readonly Channel<LogEvent> _channel;
	private readonly IHubContext<LogHub> _hubContext;

	public LogDispatcher(Channel<LogEvent> channel, IHubContext<LogHub> hubContext)
	{
		_channel = channel;
		_hubContext = hubContext;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		await foreach (var logEvent in _channel.Reader.ReadAllAsync(stoppingToken))
		{
			await _hubContext.Clients.All.SendAsync("log", logEvent, stoppingToken);
		}
	}
}


