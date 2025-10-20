using LoggerLib;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Routing;
using ILogger = LoggerLib.ILogger;

namespace Logger.Infrastructure.DependencyInjection;

public static class LoggerServiceRegistration
{
    /// <summary>
    /// Adds SignalR logger services to the DI container
    /// </summary>
    public static void AddSignalRLogger(this IServiceCollection services)
    {
        services.AddSignalR();
        services.AddSingleton<LogHub>();
        services.AddSingleton<ILogger, SignalRLogger>();
    }
}