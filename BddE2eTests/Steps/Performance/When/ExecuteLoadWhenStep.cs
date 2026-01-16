using System.Threading.Channels;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.Performance;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.Performance.When;

[Binding]
[CancelAfter(600000)] // 10 minute timeout for performance tests
public class ExecuteLoadWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);
    private readonly PerformanceTestContext _perfContext = new(scenarioContext);

    [When(@"the performance measurement is executed")]
    public async Task WhenThePerformanceMeasurementIsExecuted()
    {
        // Validate configuration
        _perfContext.ValidateConfiguration();
        
        await TestContext.Progress.WriteLineAsync(_perfContext.GetConfigurationSummary());

        // Get publisher and subscriber from regular context
        if (!_context.TryGetPublisher(out var publisher) || publisher == null)
        {
            throw new InvalidOperationException("Publisher not configured. Use 'Given a publisher is configured...' step first.");
        }

        if (!_context.TryGetReceivedMessages(out var receivedChannel) || receivedChannel == null)
        {
            throw new InvalidOperationException("Subscriber not configured. Use 'Given a subscriber is configured...' step first.");
        }

        var topic = _context.Topic;
        if (string.IsNullOrEmpty(topic))
        {
            throw new InvalidOperationException("Topic not set in context.");
        }

        var collector = _perfContext.GetOrCreateMetricsCollector();
        var loadRunner = _perfContext.GetOrCreateLoadRunner();

        // Start a background task to read from the subscriber channel and record to collector
        using var cts = new CancellationTokenSource();
        var messageReaderTask = StartMessageReaderAsync(receivedChannel, collector, cts.Token);

        await TestContext.Progress.WriteLineAsync("[Performance] Starting load test...");

        try
        {
            // Run the load test
            await loadRunner.RunAsync(
                publisher,
                _perfContext.Rate,
                _perfContext.DurationSeconds,
                topic,
                CancellationToken.None);

            // Wait for remaining messages to be received (with timeout)
            await TestContext.Progress.WriteLineAsync("[Performance] Publishing complete, waiting for remaining messages...");
            var waitTime = TimeSpan.FromSeconds(Math.Max(30, _perfContext.DurationSeconds / 2));
            await WaitForMessagesToBeReceivedAsync(collector, _perfContext.Rate * _perfContext.DurationSeconds, waitTime);
        }
        finally
        {
            // Stop the message reader
            cts.Cancel();
            try
            {
                await messageReaderTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        await TestContext.Progress.WriteLineAsync("[Performance] Generating report...");

        // Generate report
        var report = collector.GenerateReport(_perfContext.ScenarioName);

        // Write CSV files
        var reportWriter = new CsvReportWriter(_perfContext.OutputDirectory);
        var reportPaths = reportWriter.WriteReports(report);
        _perfContext.ReportPaths = reportPaths;

        // Print summary to console
        PrintReportSummary(report);

        await TestContext.Progress.WriteLineAsync($"[Performance] Reports written to: {reportPaths.Directory}");
    }

    private async Task StartMessageReaderAsync(
        Channel<TestEvent> channel,
        PerformanceMetricsCollector collector,
        CancellationToken ct)
    {
        try
        {
            await foreach (var message in channel.Reader.ReadAllAsync(ct))
            {
                var receivedTime = DateTime.UtcNow;

                // Extract sequence number from message (format: "perf-{seq}")
                if (message.Message.StartsWith("perf-") &&
                    long.TryParse(message.Message.Substring(5), out var seq))
                {
                    collector.RecordReceived(seq, receivedTime);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
    }

    private async Task WaitForMessagesToBeReceivedAsync(
        PerformanceMetricsCollector collector,
        int expectedMessages,
        TimeSpan maxWait)
    {
        var startWait = DateTime.UtcNow;
        var lastReceivedCount = 0;
        var stableCount = 0;

        while (DateTime.UtcNow - startWait < maxWait)
        {
            var report = collector.GenerateReport("temp");
            var receivedCount = report.TotalReceived;

            if (receivedCount >= expectedMessages * 0.99) // 99% threshold
            {
                await TestContext.Progress.WriteLineAsync(
                    $"[Performance] Received {receivedCount}/{expectedMessages} messages (99%+ threshold reached)");
                break;
            }

            if (receivedCount == lastReceivedCount)
            {
                stableCount++;
                if (stableCount >= 10) // No new messages for 10 checks (5 seconds)
                {
                    await TestContext.Progress.WriteLineAsync(
                        $"[Performance] No new messages for 5s, stopping wait. Received: {receivedCount}/{expectedMessages}");
                    break;
                }
            }
            else
            {
                stableCount = 0;
            }

            lastReceivedCount = receivedCount;
            await Task.Delay(500);
        }
    }

    private void PrintReportSummary(PerformanceReport report)
    {
        TestContext.Progress.WriteLine("");
        TestContext.Progress.WriteLine("╔══════════════════════════════════════════════════════════════╗");
        TestContext.Progress.WriteLine("║              PERFORMANCE TEST RESULTS                        ║");
        TestContext.Progress.WriteLine("╠══════════════════════════════════════════════════════════════╣");
        TestContext.Progress.WriteLine($"║  Scenario:     {report.ScenarioName,-46} ║");
        TestContext.Progress.WriteLine($"║  Duration:     {report.Duration.TotalSeconds:F1}s{"",-43} ║");
        TestContext.Progress.WriteLine("╠══════════════════════════════════════════════════════════════╣");
        TestContext.Progress.WriteLine("║  THROUGHPUT                                                  ║");
        TestContext.Progress.WriteLine($"║    Published:  {report.TotalPublished,10:N0} messages ({report.ThroughputPubPerSec:F1} msg/s){"",-10} ║");
        TestContext.Progress.WriteLine($"║    Received:   {report.TotalReceived,10:N0} messages ({report.ThroughputRecvPerSec:F1} msg/s){"",-10} ║");
        TestContext.Progress.WriteLine($"║    Loss:       {report.MessageLoss,10:N0} ({report.LossPercent:F2}%){"",-24} ║");
        TestContext.Progress.WriteLine("╠══════════════════════════════════════════════════════════════╣");
        TestContext.Progress.WriteLine("║  LATENCY                                                     ║");
        TestContext.Progress.WriteLine($"║    Min:        {report.LatencyMin.TotalMilliseconds,10:F2} ms{"",-34} ║");
        TestContext.Progress.WriteLine($"║    Max:        {report.LatencyMax.TotalMilliseconds,10:F2} ms{"",-34} ║");
        TestContext.Progress.WriteLine($"║    Mean:       {report.LatencyMean.TotalMilliseconds,10:F2} ms{"",-34} ║");
        TestContext.Progress.WriteLine($"║    P50:        {report.LatencyP50.TotalMilliseconds,10:F2} ms{"",-34} ║");
        TestContext.Progress.WriteLine($"║    P75:        {report.LatencyP75.TotalMilliseconds,10:F2} ms{"",-34} ║");
        TestContext.Progress.WriteLine($"║    P95:        {report.LatencyP95.TotalMilliseconds,10:F2} ms{"",-34} ║");
        TestContext.Progress.WriteLine($"║    P99:        {report.LatencyP99.TotalMilliseconds,10:F2} ms{"",-34} ║");
        TestContext.Progress.WriteLine("╠══════════════════════════════════════════════════════════════╣");
        TestContext.Progress.WriteLine("║  MEMORY                                                      ║");
        TestContext.Progress.WriteLine($"║    Min:        {report.MemoryMinBytes / (1024.0 * 1024.0),10:F2} MB{"",-33} ║");
        TestContext.Progress.WriteLine($"║    Max:        {report.MemoryMaxBytes / (1024.0 * 1024.0),10:F2} MB{"",-33} ║");
        TestContext.Progress.WriteLine($"║    Avg:        {report.MemoryAvgBytes / (1024.0 * 1024.0),10:F2} MB{"",-33} ║");
        TestContext.Progress.WriteLine("╚══════════════════════════════════════════════════════════════╝");
        TestContext.Progress.WriteLine("");
    }
}
