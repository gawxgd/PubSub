using LoggerLib.Infrastructure.SignalR;
using LoggerLib.Outbound.Adapter;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ILogger = LoggerLib.Domain.Port.ILogger;

namespace LoggerLib.Infrastructure.DependencyInjection;

public static class LoggerServiceRegistration
{
    /// <summary>
    ///     Adds SignalR logger services to the DI container
    /// </summary>
    public static IServiceCollection AddSignalRLogger(this IServiceCollection services)
    {
        services.AddSignalR();
        services.TryAddSingleton<LogHub>();
        services.TryAddSingleton<ILogger, SignalRLogger>();
        return services;
    }
}