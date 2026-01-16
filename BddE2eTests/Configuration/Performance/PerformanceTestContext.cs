using Reqnroll;

namespace BddE2eTests.Configuration.Performance;

/// <summary>
/// Context for performance tests, storing load configuration and metrics collector.
/// Works alongside ScenarioTestContext to provide performance-specific state.
/// </summary>
public class PerformanceTestContext
{
    private const string ScenarioNameKey = "PerfScenarioName";
    private const string OutputDirectoryKey = "PerfOutputDirectory";
    private const string RateKey = "PerfRate";
    private const string DurationKey = "PerfDuration";
    private const string MetricsCollectorKey = "PerfMetricsCollector";
    private const string LoadRunnerKey = "PerfLoadRunner";
    private const string ReportPathsKey = "PerfReportPaths";

    private readonly ScenarioContext _scenarioContext;

    public PerformanceTestContext(ScenarioContext scenarioContext)
    {
        _scenarioContext = scenarioContext;
    }

    /// <summary>
    /// Name of the performance scenario (used in report filenames).
    /// </summary>
    public string ScenarioName
    {
        get => _scenarioContext.TryGetValue(ScenarioNameKey, out string? name) ? name! : "unnamed_scenario";
        set => _scenarioContext.Set(value, ScenarioNameKey);
    }

    /// <summary>
    /// Directory where CSV reports will be written.
    /// </summary>
    public string OutputDirectory
    {
        get => _scenarioContext.TryGetValue(OutputDirectoryKey, out string? dir) ? dir! : "perf_reports";
        set => _scenarioContext.Set(value, OutputDirectoryKey);
    }

    /// <summary>
    /// Target message rate in messages per second.
    /// </summary>
    public int Rate
    {
        get => _scenarioContext.TryGetValue(RateKey, out int rate) ? rate : 100;
        set => _scenarioContext.Set(value, RateKey);
    }

    /// <summary>
    /// Duration of the load test in seconds.
    /// </summary>
    public int DurationSeconds
    {
        get => _scenarioContext.TryGetValue(DurationKey, out int duration) ? duration : 30;
        set => _scenarioContext.Set(value, DurationKey);
    }

    /// <summary>
    /// Gets or creates the metrics collector for this scenario.
    /// </summary>
    public PerformanceMetricsCollector GetOrCreateMetricsCollector()
    {
        if (!_scenarioContext.TryGetValue(MetricsCollectorKey, out PerformanceMetricsCollector? collector))
        {
            collector = new PerformanceMetricsCollector();
            _scenarioContext.Set(collector, MetricsCollectorKey);
        }
        return collector!;
    }

    /// <summary>
    /// Gets or creates the load runner for this scenario.
    /// </summary>
    public ConstantLoadRunner GetOrCreateLoadRunner()
    {
        if (!_scenarioContext.TryGetValue(LoadRunnerKey, out ConstantLoadRunner? runner))
        {
            var collector = GetOrCreateMetricsCollector();
            runner = new ConstantLoadRunner(collector);
            _scenarioContext.Set(runner, LoadRunnerKey);
        }
        return runner!;
    }

    /// <summary>
    /// Stores the paths to generated reports.
    /// </summary>
    public ReportPaths? ReportPaths
    {
        get => _scenarioContext.TryGetValue(ReportPathsKey, out ReportPaths? paths) ? paths : null;
        set => _scenarioContext.Set(value, ReportPathsKey);
    }

    /// <summary>
    /// Validates that all required configuration is present.
    /// </summary>
    public void ValidateConfiguration()
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(ScenarioName))
        {
            errors.Add("Scenario name is required");
        }

        if (Rate <= 0)
        {
            errors.Add("Rate must be greater than 0");
        }

        if (DurationSeconds <= 0)
        {
            errors.Add("Duration must be greater than 0");
        }

        if (errors.Count > 0)
        {
            throw new InvalidOperationException(
                $"Performance test configuration errors:\n  - {string.Join("\n  - ", errors)}");
        }
    }

    /// <summary>
    /// Gets a summary of the current configuration.
    /// </summary>
    public string GetConfigurationSummary()
    {
        return $"""
            Performance Test Configuration:
              Scenario: {ScenarioName}
              Rate: {Rate} msg/s
              Duration: {DurationSeconds} seconds
              Expected messages: {Rate * DurationSeconds}
              Output directory: {OutputDirectory}
            """;
    }
}
