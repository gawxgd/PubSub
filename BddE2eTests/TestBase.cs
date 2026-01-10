using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
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
[CancelAfter(60000)] // 60 second timeout for all tests to prevent hanging
public class TestBase(ScenarioContext scenarioContext)
{
    private const string TestConfigFileName = "config.test.json";
    private const int SchemaRegistryPort = 8081;
    private const int PublisherPort = 9096;
    private const int SubscriberPort = 9098;
    private static readonly TimeSpan PortAvailabilityTimeout = TimeSpan.FromSeconds(10);
    private const string SchemaRegistryModeTagPrefix = "schemaRegistryMode_";
    private const string DefaultSchemaRegistryCompatibilityMode = "NONE";

    private static IHost? _brokerHost;
    private static WebApplication? _schemaRegistryApp;
    private static string? _schemaDbPath;
    private static string? _commitLogDirectory;
    private static bool _loggerInitialized;
    
    public static string? CommitLogDirectory => _commitLogDirectory;

    public static async Task RestartBrokerAsync()
    {
        TestContext.Progress.WriteLine("[TestBase] === COMMIT LOG BEFORE RESTART ===");
        PrintCommitLogContents();

        if (_brokerHost != null)
        {
            TestContext.Progress.WriteLine("[TestBase] Stopping broker...");
            await _brokerHost.StopAsync();
            _brokerHost.Dispose();
            _brokerHost = null;
            TestContext.Progress.WriteLine("[TestBase] Broker stopped");
        }

        TestContext.Progress.WriteLine("[TestBase] === COMMIT LOG AFTER BROKER STOP (before restart) ===");
        PrintCommitLogContents();

        TestContext.Progress.WriteLine("[TestBase] Creating new broker host...");
        _brokerHost = CreateBrokerHostWithExistingCommitLog();

        TestContext.Progress.WriteLine("[TestBase] Starting broker...");
        await _brokerHost.StartAsync();

        TestContext.Progress.WriteLine("[TestBase] Waiting for broker startup...");
        await WaitForBrokerStartupAsync();
        TestContext.Progress.WriteLine("[TestBase] Broker restarted");

        TestContext.Progress.WriteLine("[TestBase] === COMMIT LOG AFTER RESTART ===");
        PrintCommitLogContents();
    }

    private static void PrintCommitLogContents()
    {
        if (string.IsNullOrEmpty(_commitLogDirectory))
        {
            TestContext.Progress.WriteLine("[CommitLog] No commit log directory configured");
            return;
        }

        if (!Directory.Exists(_commitLogDirectory))
        {
            TestContext.Progress.WriteLine($"[CommitLog] Directory does not exist: {_commitLogDirectory}");
            return;
        }

        TestContext.Progress.WriteLine($"[CommitLog] Directory: {_commitLogDirectory}");

        var topicDirs = Directory.GetDirectories(_commitLogDirectory);
        if (topicDirs.Length == 0)
        {
            TestContext.Progress.WriteLine("[CommitLog] No topic directories found");
            return;
        }

        foreach (var topicDir in topicDirs)
        {
            var topicName = Path.GetFileName(topicDir);
            TestContext.Progress.WriteLine($"[CommitLog] ═══════════════════════════════════════");
            TestContext.Progress.WriteLine($"[CommitLog] Topic: {topicName}");

            var logFiles = Directory.GetFiles(topicDir, "*.log").OrderBy(f => f).ToArray();
            var indexFiles = Directory.GetFiles(topicDir, "*.index").OrderBy(f => f).ToArray();

            TestContext.Progress.WriteLine(
                $"[CommitLog]   Log files: {logFiles.Length}, Index files: {indexFiles.Length}");

            foreach (var logFile in logFiles)
            {
                var fileInfo = new FileInfo(logFile);
                var fileName = Path.GetFileName(logFile);
                TestContext.Progress.WriteLine($"[CommitLog]   ───────────────────────────────────");
                TestContext.Progress.WriteLine($"[CommitLog]   File: {fileName} ({fileInfo.Length} bytes)");

                if (fileInfo.Length > 0)
                {
                    try
                    {
                        var bytes = File.ReadAllBytes(logFile);
                        var batches = ParseBatchesFromLog(bytes);

                        TestContext.Progress.WriteLine($"[CommitLog]   Total batches: {batches.Count}");

                        if (batches.Count > 0)
                        {
                            var minOffset = batches.Min(b => b.BaseOffset);
                            var maxOffset = batches.Max(b => b.LastOffset);
                            var nextOffset = maxOffset + 1;

                            TestContext.Progress.WriteLine($"[CommitLog]   Offset range: {minOffset} to {maxOffset}");
                            TestContext.Progress.WriteLine(
                                $"[CommitLog]   Next offset (high water mark): {nextOffset}");
                            TestContext.Progress.WriteLine($"[CommitLog]   ───────────────────────────────────");
                            TestContext.Progress.WriteLine($"[CommitLog]   Batch details:");

                            foreach (var batch in batches)
                            {
                                var totalSize = 24 + batch.BatchLength;
                                TestContext.Progress.WriteLine(
                                    $"[CommitLog]     Batch #{batch.Index}: baseOffset={batch.BaseOffset}, lastOffset={batch.LastOffset}, batchLen={batch.BatchLength}, recordBytes={batch.RecordBytesLength}, totalSize={totalSize}, records={batch.RecordCount}");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        TestContext.Progress.WriteLine($"[CommitLog]   Error parsing file: {ex.Message}");
                    }
                }
                else
                {
                    TestContext.Progress.WriteLine($"[CommitLog]   (empty file)");
                }
            }

            TestContext.Progress.WriteLine($"[CommitLog] ═══════════════════════════════════════");
        }
    }

    private record BatchInfo(
        int Index,
        ulong BaseOffset,
        ulong LastOffset,
        uint BatchLength,
        uint RecordBytesLength,
        int RecordCount);

    private static List<BatchInfo> ParseBatchesFromLog(byte[] logData)
    {
        var batches = new List<BatchInfo>();
        int position = 0;
        int batchIndex = 0;

        // Batch format on disk (little-endian):
        // - BaseOffset: 8 bytes (ulong)         - position 0
        // - BatchLength: 4 bytes (uint)         - position 8  (size of everything AFTER first 12 bytes, excluding LastOffset & RecordBytesLength)
        // - LastOffset: 8 bytes (ulong)         - position 12
        // - RecordBytesLength: 4 bytes (uint)   - position 20
        // - Magic: 1 byte                       - position 24
        // - CRC: 4 bytes                        - position 25
        // - Compressed: 1 byte                  - position 29
        // - Timestamp: 8 bytes                  - position 30
        // - Records: recordBytesLength bytes    - position 38
        //
        // BatchLength = 1 + 4 + 1 + 8 + recordBytesLength = 14 + recordBytesLength
        // Total batch size = 8 + 4 + 8 + 4 + batchLength = 24 + batchLength

        while (position + 24 <= logData.Length)
        {
            try
            {
                var baseOffset = BitConverter.ToUInt64(logData, position);
                var batchLength = BitConverter.ToUInt32(logData, position + 8);
                var lastOffset = BitConverter.ToUInt64(logData, position + 12);
                var recordBytesLength = BitConverter.ToUInt32(logData, position + 20);

                // Sanity checks
                if (batchLength == 0 || batchLength > 10_000_000) // Max 10MB per batch
                    break;

                // Calculate record count (rough estimate based on lastOffset - baseOffset + 1)
                var recordCount = (int)(lastOffset - baseOffset + 1);

                batches.Add(new BatchInfo(batchIndex, baseOffset, lastOffset, batchLength, recordBytesLength,
                    recordCount));

                // Move to next batch: 8 (baseOffset) + 4 (batchLength) + 8 (lastOffset) + 4 (recordBytesLength) + batchLength
                // = 24 + batchLength
                position += 24 + (int)batchLength;
                batchIndex++;
            }
            catch
            {
                break;
            }
        }

        return batches;
    }

    private static IHost CreateBrokerHostWithExistingCommitLog()
    {
        return CreateBrokerHost();
    }

    [BeforeScenario(Order = 0)]
    public async Task SetUpScenario()
    {
        TestContext.Progress.WriteLine("[TestBase] === Starting new scenario setup ===");

        var schemaRegistryCompatibilityMode = ResolveSchemaRegistryCompatibilityMode();
        TestContext.Progress.WriteLine(
            $"[TestBase] SchemaRegistry CompatibilityMode: {schemaRegistryCompatibilityMode}");

        await EnsureAllPortsAvailableAsync();

        TestContext.Progress.WriteLine("[TestBase] Creating broker host...");
        _brokerHost = CreateBrokerHostWithNewCommitLog();

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
        _schemaRegistryApp = CreateSchemaRegistryHost(schemaRegistryCompatibilityMode);

        TestContext.Progress.WriteLine("[TestBase] Starting schema registry...");
        await _schemaRegistryApp.StartAsync();
        TestContext.Progress.WriteLine($"[TestBase] SchemaRegistry started on port {SchemaRegistryPort}");

        TestContext.Progress.WriteLine("[TestBase] Setup complete!");
    }

    private string ResolveSchemaRegistryCompatibilityMode()
    {
        var tags = scenarioContext.ScenarioInfo.Tags;
        var modeTag = tags.FirstOrDefault(t =>
            t.StartsWith(SchemaRegistryModeTagPrefix, StringComparison.OrdinalIgnoreCase));

        if (string.IsNullOrWhiteSpace(modeTag))
        {
            return DefaultSchemaRegistryCompatibilityMode;
        }

        var mode = modeTag[SchemaRegistryModeTagPrefix.Length..].Trim();
        if (string.IsNullOrWhiteSpace(mode))
        {
            return DefaultSchemaRegistryCompatibilityMode;
        }

        // normalize to expected enum strings (BACKWARD/FORWARD/FULL/NONE)
        return mode.ToUpper(CultureInfo.InvariantCulture);
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

        // Cleanup schema registry SQLite DB
        if (!string.IsNullOrWhiteSpace(_schemaDbPath) && File.Exists(_schemaDbPath))
        {
            try
            {
                File.Delete(_schemaDbPath);
            }
            catch
            {
                /* ignore cleanup errors */
            }

            _schemaDbPath = null;
        }

        // Cleanup commit log
        if (_commitLogDirectory != null && Directory.Exists(_commitLogDirectory))
        {
            try
            {
                Directory.Delete(_commitLogDirectory, recursive: true);
            }
            catch
            {
                /* ignore cleanup errors */
            }

            _commitLogDirectory = null;
        }

        TestContext.Progress.WriteLine("[TestBase] Teardown complete!");
    }

    private static IHost CreateBrokerHostWithNewCommitLog()
    {
        _commitLogDirectory = CreateTemporaryCommitLogDirectory();
        return CreateBrokerHost();
    }

    private static IHost CreateBrokerHost()
    {
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

    private static WebApplication CreateSchemaRegistryHost(string schemaRegistryCompatibilityMode)
    {
        _schemaDbPath = Path.Combine(Path.GetTempPath(), $"bdd-schema-registry-{Guid.NewGuid()}.db");
        var sqliteConnectionString = $"Data Source={_schemaDbPath}";

        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls($"http://127.0.0.1:{SchemaRegistryPort}");

        builder.Services.AddControllers()
            .AddApplicationPart(typeof(ServiceRegistration).Assembly);

        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["SchemaRegistry:StorageType"] = "Sqlite",
            ["SchemaRegistry:ConnectionString"] = sqliteConnectionString,
            ["SchemaRegistry:CompatibilityMode"] = schemaRegistryCompatibilityMode
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

    private static async Task WaitForPortAvailabilityAsync(int port, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            try
            {
                using var listener = new TcpListener(IPAddress.Loopback, port);
                listener.Start();
                listener.Stop();
                TestContext.Progress.WriteLine($"[TestBase] Port {port} is available");
                return;
            }
            catch (SocketException)
            {
                TestContext.Progress.WriteLine($"[TestBase] Port {port} is not available yet, waiting...");
                await Task.Delay(100);
            }
        }

        throw new TimeoutException($"Port {port} is not available after {timeout.TotalSeconds}s. " +
                                   "Try running: pkill -f 'dotnet.*BddE2eTests' to kill lingering test processes.");
    }

    private static async Task EnsureAllPortsAvailableAsync()
    {
        TestContext.Progress.WriteLine("[TestBase] Checking port availability...");
        
        var ports = new[] { PublisherPort, SubscriberPort, SchemaRegistryPort };
        foreach (var port in ports)
        {
            await WaitForPortAvailabilityAsync(port, PortAvailabilityTimeout);
        }
        
        TestContext.Progress.WriteLine("[TestBase] All ports are available!");
    }
}