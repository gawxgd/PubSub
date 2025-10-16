using MessageBroker.Infrastructure.DependencyInjection;
using MessageBroker.Infrastructure.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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
                // logging forwarder
                services.AddHttpClient(HttpLogDispatcher.HttpClientName);
                services.AddSingleton(System.Threading.Channels.Channel.CreateUnbounded<HttpLogEvent>());
                services.AddHostedService<HttpLogDispatcher>();
                services.Configure<LoggerOptions>(context.Configuration.GetSection("Logger"));
                services.AddSingleton<ILoggerProvider, HttpLoggerProvider>();
            })
            .ConfigureLogging((context, logging) =>
            {
                // keep defaults; provider added via DI
            });

        return builder;
    }
}