using LoggerLib.Outbound.Adapter;
using MessageBroker.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;
using ILogger = LoggerLib.Domain.Port.ILogger;

namespace BddE2eTests;

[SetUpFixture]
public class TestBase
{
    private const string TestConfigFileName = "config.test.json";

    private static IHost _brokerHost;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _brokerHost = CreateBrokerHost();
        InitializeLogger();
        await _brokerHost.StartAsync();
        await WaitForBrokerStartupAsync();
        Console.WriteLine("MessageBroker started");
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await _brokerHost.StopAsync();
        _brokerHost.Dispose();
        Console.WriteLine("MessageBroker stopped");
    }

    private static IHost CreateBrokerHost()
    {
        var commitLogDirectory = CreateTemporaryCommitLogDirectory();

        return Host.CreateDefaultBuilder([])
            .ConfigureMessageBroker([])
            .ConfigureAppConfiguration((config) =>
            {
                config
                    .AddJsonFile(TestConfigFileName, optional: false, reloadOnChange: false)
                    .AddInMemoryCollection(new Dictionary<string, string?>
                    {
                        ["CommitLog:Directory"] = commitLogDirectory
                    });
            })
            .Build();
    }

    private static string CreateTemporaryCommitLogDirectory()
    {
        return Path.Combine(Path.GetTempPath(), $"bdd-commit-log-{Guid.NewGuid()}");
    }

    private static void InitializeLogger()
    {
        var logger = GetLogger();
        AutoLoggerFactory.Initialize(logger);
    }

    private static ILogger GetLogger()
    {
        return _brokerHost.Services.GetRequiredService<ILogger>();
    }

    private static async Task WaitForBrokerStartupAsync()
    {
        var lifetime = _brokerHost.Services.GetRequiredService<IHostApplicationLifetime>();
        var startupSignal = new TaskCompletionSource();

        await using var registration = lifetime.ApplicationStarted.Register(() => startupSignal.TrySetResult());

        await startupSignal.Task;
    }
}