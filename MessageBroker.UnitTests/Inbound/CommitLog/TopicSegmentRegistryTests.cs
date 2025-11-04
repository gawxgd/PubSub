using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Inbound.CommitLog.TopicSegmentManager;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class TopicSegmentRegistryTests
{
    [Fact]
    public void Should_Return_Initial_Active_Segment()
    {
        var seg = new LogSegment("a.log", "a.index", "a.time", 0, 0);
        var manager = new TopicSegmentRegistry(seg, 0);

        manager.GetActiveSegment().Should().Be(seg);
    }

    [Fact]
    public void Should_Update_Active_Segment()
    {
        var seg1 = new LogSegment("a.log", "a.index", "a.time", 0, 0);
        var seg2 = new LogSegment("b.log", "b.index", "b.time", 100, 100);
        var manager = new TopicSegmentRegistry(seg1, 0);

        manager.UpdateActiveSegment(seg2);

        manager.GetActiveSegment().Should().Be(seg2);
    }

    [Fact]
    public void Should_Get_And_Update_HighWaterMark()
    {
        var seg = new LogSegment("a.log", "a.index", "a.time", 0, 0);
        var manager = new TopicSegmentRegistry(seg, 5);

        manager.GetHighWaterMark().Should().Be(5UL);
        manager.UpdateCurrentOffset(42);
        manager.GetHighWaterMark().Should().Be(42UL);
    }
}