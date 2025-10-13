using MessageBroker.TcpServer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MessageBroker.Infrastructure;

public static class ServiceRegistration
{
    /// <summary>
    ///     Register all tcp services
    /// </summary>
    public static void AddTcpServices(this IServiceCollection services)
    {
        services.AddSingleton<CreateSocketUseCase>();
        services.AddHostedService<TcpServerService>();
    }

    /// <summary>
    ///     Register all options / configuration objects
    /// </summary>
    public static void AddBrokerOptions(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<TcpServerOptions>(configuration);
    }
}