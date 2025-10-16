using System.Threading.Channels;
using Logger.Domain;
using Logger.Realtime;
using Microsoft.AspNetCore.Builder;

namespace Logger.Infrastructure;

public static class EndpointMappingExtensions
{
	public static void MapLogEndpoints(this WebApplication app)
	{
		app.MapGet("/", () => "Logger service up");

		app.MapPost("/ingest", async (LogEvent logEvent, Channel<LogEvent> logChannel) =>
		{
			await logChannel.Writer.WriteAsync(logEvent);
			return Results.Accepted();
		});

		app.MapPost("/ingest/batch", async (IEnumerable<LogEvent> logEvents, Channel<LogEvent> logChannel) =>
		{
			foreach (var e in logEvents)
			{
				await logChannel.Writer.WriteAsync(e);
			}
			return Results.Accepted();
		});

		app.MapHub<LogHub>("/hubs/logs");
	}
}


