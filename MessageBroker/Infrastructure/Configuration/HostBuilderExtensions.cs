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
            });

        return builder;
    }
}