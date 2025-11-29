using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using NSubstitute;

namespace BddE2eTests.Configuration;

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

