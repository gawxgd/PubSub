using LoggerLib.Infrastructure.SignalR;
using Microsoft.Extensions.DependencyInjection;
using ILogger = LoggerLib.Domain.Port.ILogger;

namespace LoggerLib.Infrastructure.DependencyInjection;

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