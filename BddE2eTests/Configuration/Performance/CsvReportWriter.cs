using System.Globalization;
using System.Text;

namespace BddE2eTests.Configuration.Performance;

/// <summary>
/// Writes performance report data to CSV files.
/// Generates three files per scenario: summary, latencies, and timeline.
/// </summary>
public class CsvReportWriter
{
    private readonly string _outputDirectory;

    public CsvReportWriter(string outputDirectory)
    {
        _outputDirectory = outputDirectory;
    }

    /// <summary>
    /// Writes all CSV reports for the given performance report.
    /// </summary>
    /// <param name="report">The performance report data</param>
    /// <returns>Paths to the generated files</returns>
    public ReportPaths WriteReports(PerformanceReport report)
    {
        // Create output directory with timestamp
        var timestamp = report.StartTime.ToString("yyyyMMdd_HHmmss");
        var scenarioDir = Path.Combine(_outputDirectory, $"{report.ScenarioName}_{timestamp}");
        Directory.CreateDirectory(scenarioDir);

        var summaryPath = Path.Combine(scenarioDir, $"{report.ScenarioName}_summary.csv");
        var latenciesPath = Path.Combine(scenarioDir, $"{report.ScenarioName}_latencies.csv");
        var timelinePath = Path.Combine(scenarioDir, $"{report.ScenarioName}_timeline.csv");

        WriteSummaryReport(report, summaryPath);
        WriteLatenciesReport(report, latenciesPath);
        WriteTimelineReport(report, timelinePath);

        Console.WriteLine($"[CsvReportWriter] Reports written to: {scenarioDir}");
        Console.WriteLine($"[CsvReportWriter]   - {Path.GetFileName(summaryPath)}");
        Console.WriteLine($"[CsvReportWriter]   - {Path.GetFileName(latenciesPath)}");
        Console.WriteLine($"[CsvReportWriter]   - {Path.GetFileName(timelinePath)}");

        return new ReportPaths(summaryPath, latenciesPath, timelinePath, scenarioDir);
    }

    private void WriteSummaryReport(PerformanceReport report, string path)
    {
        var sb = new StringBuilder();

        // Header
        sb.AppendLine("scenario,start,end,duration_sec,published,received,loss,loss_pct,throughput_pub,throughput_recv,lat_min_ms,lat_max_ms,lat_mean_ms,lat_p50_ms,lat_p75_ms,lat_p95_ms,lat_p99_ms,mem_min_mb,mem_max_mb,mem_avg_mb");

        // Data row
        sb.AppendLine(string.Join(",",
            report.ScenarioName,
            report.StartTime.ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture),
            report.EndTime.ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture),
            report.Duration.TotalSeconds.ToString("F2", CultureInfo.InvariantCulture),
            report.TotalPublished,
            report.TotalReceived,
            report.MessageLoss,
            report.LossPercent.ToString("F2", CultureInfo.InvariantCulture),
            report.ThroughputPubPerSec.ToString("F2", CultureInfo.InvariantCulture),
            report.ThroughputRecvPerSec.ToString("F2", CultureInfo.InvariantCulture),
            report.LatencyMin.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture),
            report.LatencyMax.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture),
            report.LatencyMean.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture),
            report.LatencyP50.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture),
            report.LatencyP75.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture),
            report.LatencyP95.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture),
            report.LatencyP99.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture),
            BytesToMB(report.MemoryMinBytes).ToString("F2", CultureInfo.InvariantCulture),
            BytesToMB(report.MemoryMaxBytes).ToString("F2", CultureInfo.InvariantCulture),
            BytesToMB(report.MemoryAvgBytes).ToString("F2", CultureInfo.InvariantCulture)
        ));

        File.WriteAllText(path, sb.ToString());
    }

    private void WriteLatenciesReport(PerformanceReport report, string path)
    {
        var sb = new StringBuilder();

        // Header
        sb.AppendLine("sequence,published_utc,received_utc,latency_ms");

        // Data rows
        foreach (var sample in report.LatencySamples)
        {
            sb.AppendLine(string.Join(",",
                sample.SequenceNumber,
                sample.PublishedUtc.ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture),
                sample.ReceivedUtc.ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture),
                sample.Latency.TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture)
            ));
        }

        File.WriteAllText(path, sb.ToString());
    }

    private void WriteTimelineReport(PerformanceReport report, string path)
    {
        var sb = new StringBuilder();

        // Header
        sb.AppendLine("second,published,received,throughput_pub,throughput_recv,avg_latency_ms");

        // Data rows
        foreach (var dataPoint in report.TimelineData)
        {
            sb.AppendLine(string.Join(",",
                dataPoint.Second,
                dataPoint.Published,
                dataPoint.Received,
                dataPoint.ThroughputPub.ToString("F2", CultureInfo.InvariantCulture),
                dataPoint.ThroughputRecv.ToString("F2", CultureInfo.InvariantCulture),
                dataPoint.AvgLatencyMs.ToString("F2", CultureInfo.InvariantCulture)
            ));
        }

        File.WriteAllText(path, sb.ToString());
    }

    private static double BytesToMB(long bytes) => bytes / (1024.0 * 1024.0);
}

public record ReportPaths(
    string SummaryPath,
    string LatenciesPath,
    string TimelinePath,
    string Directory);
