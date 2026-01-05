using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.Broker.When;

[Binding]
public class BrokerRestartWhenStep
{
    [When(@"the broker restarts")]
    public async Task WhenTheBrokerRestarts()
    {
        await TestContext.Progress.WriteLineAsync("[When Step] Restarting broker...");
        
        await TestBase.RestartBrokerAsync();
        
        await TestContext.Progress.WriteLineAsync("[When Step] Broker restarted!");
    }
}

