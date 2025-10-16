using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Logger.Domain;
using Logger.Realtime;
using Microsoft.Extensions.DependencyInjection;

namespace Logger.Infrastructure;

public static class ServiceCollectionExtensions
{
	public static IServiceCollection AddLoggerCore(this IServiceCollection services)
	{
		services.AddCors(options =>
		{
			options.AddPolicy("AllowFrontendAndServices", policy =>
			{
				policy
					.AllowAnyHeader()
					.AllowAnyMethod()
					.SetIsOriginAllowed(_ => true)
					.AllowCredentials();
			});
		});

		services.ConfigureHttpJsonOptions(o =>
		{
			o.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
			o.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
		});

		services.AddSignalR();
		services.AddSingleton(sp => Channel.CreateUnbounded<LogEvent>(new UnboundedChannelOptions
		{
			SingleReader = false,
			SingleWriter = false,
			AllowSynchronousContinuations = false
		}));
		services.AddHostedService<LogDispatcher>();
		return services;
	}
}


