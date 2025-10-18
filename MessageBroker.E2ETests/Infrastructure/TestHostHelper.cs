using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.Repositories;
using MessageBroker.Inbound.Adapter;
using MessageBroker.Inbound.TcpServer.Service;
using MessageBroker.Infrastructure.Configuration.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MessageBroker.E2ETests.Infrastructure;

public static class TestHostHelper
{
    public static IHost CreateTestHost(int? port = null, string address = "127.0.0.1")
    {
        var actualPort = port ?? PortManager.GetNextPort();

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Port"] = actualPort.ToString(),
                ["Address"] = address,
                ["MaxRequestSizeInByte"] = "512",
                ["InlineCompletions"] = "false",
                ["SocketPolling"] = "false",
                ["Backlog"] = "100"
            })
            .Build();

        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<IConnectionRepository, InMemoryConnectionRepository>();
                services.AddSingleton<IConnectionManager, ConnectionManager>();
                services.AddSingleton<CreateSocketUseCase>();
                services.AddHostedService<TcpServer>();
                services.Configure<TcpServerOptions>(configuration);
            })
            .Build();
    }

    public static async Task<IHost> CreateAndStartTestHostAsync(int? port = null, string address = "127.0.0.1")
    {
        var host = CreateTestHost(port, address);
        await host.StartAsync();
        return host;
    }
}