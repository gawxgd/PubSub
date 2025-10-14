using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.Repositories;
using MessageBroker.Inbound.Adapter;
using MessageBroker.Inbound.TcpServer.Service;
using MessageBroker.Infrastructure.Configuration.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MessageBroker.Infrastructure.DependencyInjection;

public static class ServiceRegistration
{
    /// <summary>
    ///     Register all tcp services
    /// </summary>
    public static void AddTcpServices(this IServiceCollection services)
    {
        // Connection management
        services.AddSingleton<IConnectionRepository, InMemoryConnectionRepository>();
        services.AddSingleton<IConnectionManager, ConnectionManager>();
        // TCP server
        services.AddSingleton<CreateSocketUseCase>();
        services.AddHostedService<TcpServer>();
    }

    /// <summary>
    ///     Register all options / configuration objects
    /// </summary>
    public static void AddBrokerOptions(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<TcpServerOptions>(configuration);
    }
}