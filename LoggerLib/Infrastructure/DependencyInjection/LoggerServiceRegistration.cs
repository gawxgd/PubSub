using LoggerLib.Infrastructure.SignalR;
using LoggerLib.Outbound.Adapter;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ILogger = LoggerLib.Domain.Port.ILogger;
using SignalRLogger = LoggerLib.Outbound.Adapter.SignalRLogger;

namespace LoggerLib.Infrastructure.DependencyInjection;

public static class LoggerServiceRegistration
{

    public static IServiceCollection AddSignalRLogger(this IServiceCollection services)
    {
        services.AddSignalR();
        services.TryAddSingleton<LogHub>();
        services.TryAddSingleton<ILogger, SignalRLogger>();
        return services;
    }
}
