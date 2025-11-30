using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Inbound.CommitLog.TopicSegmentManager;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class TopicSegmentRegistryFactoryTests
{
    [Fact]
    public void GetOrCreate_Should_Create_And_Cache_By_Topic()
    {
        var factory = Substitute.For<ILogSegmentFactory>();
        factory.CreateLogSegment(Arg.Any<string>(), Arg.Any<ulong>())
            .Returns(c => new LogSegment("a.log", "a.index", "a.time", (ulong)c[1], (ulong)c[1]));

        var registry = new TopicSegmentRegistryFactory(factory);

        var m1 = registry.GetOrCreate("t1", "dir", 0);
        var m2 = registry.GetOrCreate("t1", "dir", 0);
        var m3 = registry.GetOrCreate("t2", "dir", 0);

        m1.Should().BeSameAs(m2);
        m3.Should().NotBeSameAs(m1);
    }
}