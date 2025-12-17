using System.Net.Http.Json;
using System.Text.Json;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;
using Reqnroll;
using SchemaRegistry;
using ILogger = LoggerLib.Domain.Port.ILogger;

namespace BddE2eTests;

[Binding]
public class TestBase
{
    private const string TestConfigFileName = "config.test.json";
    private const int SchemaRegistryPort = 8081;

    private static IHost? _brokerHost;
    private static WebApplication? _schemaRegistryApp;
    private static string? _schemaStorePath;
    private static string? _commitLogDirectory;
    private static bool _loggerInitialized;

    [BeforeScenario(Order = 0)]
    public async Task SetUpScenario()
    {
        TestContext.Progress.WriteLine("[TestBase] === Starting new scenario setup ===");
        
        TestContext.Progress.WriteLine("[TestBase] Creating broker host...");
        _brokerHost = CreateBrokerHost();
        
        if (!_loggerInitialized)
        {
            TestContext.Progress.WriteLine("[TestBase] Initializing logger...");
            InitializeLogger();
            _loggerInitialized = true;
        }
        
        TestContext.Progress.WriteLine("[TestBase] Starting broker...");
        await _brokerHost.StartAsync();
        
        TestContext.Progress.WriteLine("[TestBase] Waiting for broker startup...");
        await WaitForBrokerStartupAsync();
        TestContext.Progress.WriteLine("[TestBase] MessageBroker started");

        TestContext.Progress.WriteLine("[TestBase] Creating schema registry...");
        _schemaRegistryApp = CreateSchemaRegistryHost();
        
        TestContext.Progress.WriteLine("[TestBase] Starting schema registry...");
        await _schemaRegistryApp.StartAsync();
        TestContext.Progress.WriteLine($"[TestBase] SchemaRegistry started on port {SchemaRegistryPort}");

        TestContext.Progress.WriteLine("[TestBase] Registering test schemas...");
        await RegisterTestSchemasAsync();
        TestContext.Progress.WriteLine("[TestBase] Setup complete!");
    }

    [AfterScenario(Order = 1000)]
    public async Task TearDownScenario()
    {
        TestContext.Progress.WriteLine("[TestBase] === Tearing down scenario ===");
        
        if (_schemaRegistryApp != null)
        {
            await _schemaRegistryApp.StopAsync();
            await _schemaRegistryApp.DisposeAsync();
            _schemaRegistryApp = null;
            TestContext.Progress.WriteLine("[TestBase] SchemaRegistry stopped");
        }

        if (_brokerHost != null)
        {
            await _brokerHost.StopAsync();
            _brokerHost.Dispose();
            _brokerHost = null;
            TestContext.Progress.WriteLine("[TestBase] MessageBroker stopped");
        }

        // Cleanup schema store
        if (_schemaStorePath != null && Directory.Exists(_schemaStorePath))
        {
            try
            {
                Directory.Delete(_schemaStorePath, recursive: true);
            }
            catch { /* ignore cleanup errors */ }
            _schemaStorePath = null;
        }
        
        // Cleanup commit log
        if (_commitLogDirectory != null && Directory.Exists(_commitLogDirectory))
        {
            try
            {
                Directory.Delete(_commitLogDirectory, recursive: true);
            }
            catch { /* ignore cleanup errors */ }
            _commitLogDirectory = null;
        }
        
        TestContext.Progress.WriteLine("[TestBase] Teardown complete!");
    }

    private static IHost CreateBrokerHost()
    {
        _commitLogDirectory = CreateTemporaryCommitLogDirectory();

        return Host.CreateDefaultBuilder([])
            .ConfigureMessageBroker([])
            .ConfigureAppConfiguration((config) =>
            {
                config
                    .AddJsonFile(TestConfigFileName, optional: false, reloadOnChange: false)
                    .AddInMemoryCollection(new Dictionary<string, string?>
                    {
                        ["CommitLog:Directory"] = _commitLogDirectory
                    });
            })
            .Build();
    }

    private static WebApplication CreateSchemaRegistryHost()
    {
        _schemaStorePath = Path.Combine(Path.GetTempPath(), $"bdd-schema-store-{Guid.NewGuid()}");
        Directory.CreateDirectory(_schemaStorePath);

        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls($"http://127.0.0.1:{SchemaRegistryPort}");

        builder.Services.AddControllers()
            .AddApplicationPart(typeof(ServiceRegistration).Assembly);

        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["SchemaRegistry:FileStoreFolderPath"] = _schemaStorePath,
            ["SchemaRegistry:CompatibilityMode"] = "NONE"
        });

        builder.Services.AddSchemaRegistryServices(builder.Configuration);

        var app = builder.Build();

        app.UseRouting();
        app.MapControllers();

        return app;
    }

    private static string CreateTemporaryCommitLogDirectory()
    {
        return Path.Combine(Path.GetTempPath(), $"bdd-commit-log-{Guid.NewGuid()}");
    }

    private static void InitializeLogger()
    {
        var logger = new Configuration.ConsoleTestLogger();
        AutoLoggerFactory.Initialize(logger);
    }

    private static async Task WaitForBrokerStartupAsync()
    {
        var lifetime = _brokerHost!.Services.GetRequiredService<IHostApplicationLifetime>();
        var startupSignal = new TaskCompletionSource();

        await using var registration = lifetime.ApplicationStarted.Register(() => startupSignal.TrySetResult());

        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(30));
        var completedTask = await Task.WhenAny(startupSignal.Task, timeoutTask);
        
        if (completedTask == timeoutTask)
        {
            throw new TimeoutException("Broker startup timed out after 30 seconds");
        }
    }

    private static async Task RegisterTestSchemasAsync()
    {
        using var httpClient = new HttpClient 
        { 
            BaseAddress = new Uri($"http://127.0.0.1:{SchemaRegistryPort}"),
            Timeout = TimeSpan.FromSeconds(10)
        };

        var testEventSchema = new
        {
            schema = JsonSerializer.Serialize(new
            {
                type = "record",
                name = "TestEvent",
                @namespace = "BddE2eTests.Configuration",
                fields = new[]
                {
                    new { name = "Message", type = "string" },
                    new { name = "Topic", type = "string" }
                }
            })
        };

        var topics = new[] { "default", "test-topic", "custom-topic" };

        foreach (var topic in topics)
        {
            var response = await httpClient.PostAsJsonAsync($"schema/topic/{topic}", testEventSchema);
            if (response.IsSuccessStatusCode)
            {
                TestContext.Progress.WriteLine($"[TestBase] Registered schema for topic: {topic}");
            }
            else
            {
                var error = await response.Content.ReadAsStringAsync();
                TestContext.Progress.WriteLine($"[TestBase] Failed to register schema for {topic}: {error}");
            }
        }
    }
}
