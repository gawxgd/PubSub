using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.SchemaRegistry.When;

[Binding]
public class SchemaRegistryRestartWhenStep
{
    [When(@"schema registry restarts")]
    public async Task WhenSchemaRegistryRestarts()
    {
        await TestContext.Progress.WriteLineAsync("[When Step] Restarting schema registry...");
        await TestBase.RestartSchemaRegistryAsync();
        await TestContext.Progress.WriteLineAsync("[When Step] Schema registry restarted!");
    }
}

