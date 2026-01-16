using BddE2eTests.Configuration.Performance;
using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.Performance.Given;

[Binding]
public class LoadConfigurationGivenStep(ScenarioContext scenarioContext)
{
    private readonly PerformanceTestContext _perfContext = new(scenarioContext);

    [Given(@"the scenario name is ""(.*)""")]
    public async Task GivenTheScenarioNameIs(string name)
    {
        await TestContext.Progress.WriteLineAsync($"[Performance] Setting scenario name: {name}");
        _perfContext.ScenarioName = name;
    }

    [Given(@"the report output directory is ""(.*)""")]
    public async Task GivenTheReportOutputDirectoryIs(string path)
    {
        await TestContext.Progress.WriteLineAsync($"[Performance] Setting output directory: {path}");
        _perfContext.OutputDirectory = path;
    }

    [Given(@"a constant load of (\d+) messages per second for (\d+) seconds")]
    public async Task GivenAConstantLoadOfMessagesPerSecondForSeconds(int rate, int duration)
    {
        await TestContext.Progress.WriteLineAsync($"[Performance] Configuring load: {rate} msg/s for {duration}s");
        _perfContext.Rate = rate;
        _perfContext.DurationSeconds = duration;
        
        var expectedMessages = rate * duration;
        await TestContext.Progress.WriteLineAsync($"[Performance] Expected total messages: {expectedMessages}");
    }

    [Given(@"a load rate of (\d+) messages per second")]
    public async Task GivenALoadRateOfMessagesPerSecond(int rate)
    {
        await TestContext.Progress.WriteLineAsync($"[Performance] Setting rate: {rate} msg/s");
        _perfContext.Rate = rate;
    }

    [Given(@"a test duration of (\d+) seconds")]
    public async Task GivenATestDurationOfSeconds(int duration)
    {
        await TestContext.Progress.WriteLineAsync($"[Performance] Setting duration: {duration}s");
        _perfContext.DurationSeconds = duration;
    }
}
