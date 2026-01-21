using LoggerLib.Infrastructure.DependencyInjection;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Inbound.Adapter;
using MessageBroker.Inbound.TcpServer.Service;
using MessageBroker.Infrastructure.Configuration.Options;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using MessageBroker.Infrastructure.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ILogger = LoggerLib.Domain.Port.ILogger;

namespace MessageBroker.E2ETests.Infrastructure;

public static class TestHostHelper
{
    public static IHost CreateTestHost(int? port = null, string address = "127.0.0.1")
    {
        var actualPort = port ?? PortManager.GetNextPort();

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                // TCP Server options
                ["Server:PublisherPort"] = actualPort.ToString(),
                ["Server:SubscriberPort"] = (actualPort + 1).ToString(),
                ["Server:Address"] = address,
                ["Server:MaxRequestSizeInByte"] = "512",
                ["Server:InlineCompletions"] = "false",
                ["Server:Backlog"] = "100",

                // Commit Log options
                ["CommitLog:Directory"] = Path.Combine(Path.GetTempPath(), "e2e-commit-log-" + Guid.NewGuid()),
                ["CommitLog:MaxSegmentBytes"] = "10485760", // 10 MB
                ["CommitLog:IndexIntervalBytes"] = "4096",
                ["CommitLog:FileBufferSize"] = "65536",
                ["CommitLog:TimeIndexIntervalMs"] = "4096",
                ["CommitLog:FlushIntervalMs"] = "100",

                // Default topic configuration
                ["CommitLogTopics:0:Name"] = "default",
                ["CommitLogTopics:0:BaseOffset"] = "0",
                ["CommitLogTopics:0:FlushIntervalMs"] = "100"
            })
            .Build();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                // TCP services
                services.AddTcpServices();
                services.AddBrokerOptions(configuration);

                // Commit Log services - THIS WAS MISSING!
                services.AddCommitLogServices();
                services.Configure<CommitLogOptions>(configuration.GetSection("CommitLog"));
                services.Configure<List<CommitLogTopicOptions>>(configuration.GetSection("CommitLogTopics"));

                // Logger
                services.AddSignalRLogger();
            })
            .Build();

        // Initialize AutoLoggerFactory with the registered logger
        var logger = host.Services.GetRequiredService<ILogger>();
        AutoLoggerFactory.Initialize(logger);

        return host;
    }

    public static async Task<IHost> CreateAndStartTestHostAsync(int? port = null, string address = "127.0.0.1")
    {
        var host = CreateTestHost(port, address);
        await host.StartAsync();
        return host;
    }
}