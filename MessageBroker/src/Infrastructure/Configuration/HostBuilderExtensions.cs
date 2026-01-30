using LoggerLib;
using LoggerLib.Infrastructure.DependencyInjection;
using LoggerLib.Infrastructure.SignalR;
using MessageBroker.Infrastructure.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace MessageBroker.Infrastructure.Configuration;

public static class HostBuilderExtensions
{
    private const string ConfigFileName = "config.json";
    private const string EnvironmentVariablesPrefix = "MessageBrokerEnv_";

    public static IHostBuilder ConfigureMessageBroker(this IHostBuilder builder, string[] args)
    {
        builder
            .ConfigureAppConfiguration((context, config) =>
            {
                config
                    .SetBasePath(AppContext.BaseDirectory)
                    .AddJsonFile(ConfigFileName, true, true)
                    .AddEnvironmentVariables(EnvironmentVariablesPrefix)
                    .AddCommandLine(args);
            })
            .ConfigureServices((context, services) =>
            {
                services.AddBrokerOptions(context.Configuration);
                services.AddTcpServices();
                services.AddSignalRLogger();
                services.AddCommitLogServices();
                services.AddCors(options =>
                {
                    options.AddDefaultPolicy(builder =>
                    {
                        builder.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
                    });
                });
            })
            .ConfigureWebHostDefaults(webBuilder =>
            {
                var configBuilder = new ConfigurationBuilder()
                    .SetBasePath(AppContext.BaseDirectory)
                    .AddJsonFile(ConfigFileName, true, true)
                    .AddEnvironmentVariables(EnvironmentVariablesPrefix)
                    .AddCommandLine(args);

                var config = configBuilder.Build();
                var loggerPort = config.GetValue<int>("LoggerPort", 5001);

                webBuilder.UseUrls($"http:
                webBuilder.Configure(app =>
                {
                    app.UseCors();
                    app.UseRouting();
                    app.UseEndpoints(endpoints => 
                    { 
                        endpoints.MapLogger();
                        endpoints.MapGet("/api/statistics", async context =>
                        {
                            var statisticsService = context.RequestServices.GetRequiredService<MessageBroker.Domain.Port.IStatisticsService>();
                            var statistics = statisticsService.GetStatistics();
                            await context.Response.WriteAsJsonAsync(statistics);
                        });
                    });
                });
            });

        return builder;
    }
}
