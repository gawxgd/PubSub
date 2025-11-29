using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using NSubstitute;
using NUnit.Framework;

namespace BddE2eTests.Steps;

[SetUpFixture]
public class TestBase
{
    [OneTimeSetUp]
    public void InitializeLogger()
    {
        //ToDo should we initalize it like this?
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);
    }
}

