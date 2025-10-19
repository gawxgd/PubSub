using LoggerLib;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Infrastructure.DependencyInjection;

    public static class LoggerRegistration
    {
        public static void AddLoggerServices(this IServiceCollection services)
        {
            services.AddSignalR();
            services.AddSingleton<ILogger, SignalRLogger>();
        }
    }