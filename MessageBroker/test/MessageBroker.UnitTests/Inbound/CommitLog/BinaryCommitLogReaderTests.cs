using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;
using MessageBroker.Inbound.CommitLog;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class BinaryCommitLogReaderTests
{
    //ToDo make better coverage
    private readonly ILogSegmentFactory _segmentFactory = Substitute.For<ILogSegmentFactory>();
    private readonly ITopicSegmentRegistry _registry = Substitute.For<ITopicSegmentRegistry>();

    public BinaryCommitLogReaderTests()
    {
        AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
    }

    [Fact]
    public void ReadRecords_Should_Return_Whole_Batch_From_Offset()
    {
        var segment = new LogSegment("a.log", "a.index", "a.timeindex", 0, 12);
        _registry.GetActiveSegment().Returns(segment);
        _registry.GetSegmentContainingOffset(10).Returns(segment);

        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            10,
            new List<LogRecord>
            {
                new LogRecord(10, 100, new byte[] { 1 }),
                new LogRecord(11, 101, new byte[] { 2 })
            },
            false);

        var segReader = Substitute.For<ILogSegmentReaderM>();
        segReader.ReadBatch(10).Returns(batch);

        _segmentFactory.CreateReaderM(segment).Returns(segReader);

        var reader = new BinaryCommitLogReaderM(_segmentFactory, _registry);

        var records = reader.ReadRecordBatch(10)!.Records.ToList();

        records.Should().HaveCount(2);
        records.Select(r => r.Offset).Should().BeEquivalentTo([10UL, 11UL]);
    }

    [Fact]
    public void ReadFromTimestamp_Should_Return_Whole_First_Batch()
    {
        //ToDo test after implementation
        var segment = new LogSegment("a.log", "a.index", "a.timeindex", 0, 0);
        _registry.GetActiveSegment().Returns(segment);

        var firstBatch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            20,
            new List<LogRecord>
            {
                new LogRecord(20, 200, new byte[] { 1 }),
                new LogRecord(21, 201, new byte[] { 2 })
            },
            false);

        var segReader = Substitute.For<ILogSegmentReaderM>();
        segReader.ReadFromTimestamp(200).Returns(new[] { firstBatch });

        _segmentFactory.CreateReaderM(segment).Returns(segReader);

        var reader = new BinaryCommitLogReaderM(_segmentFactory, _registry);

        var records = reader.ReadFromTimestamp(200).ToList();

        records.Should().HaveCount(2);
        records.Select(r => r.Offset).Should().BeEquivalentTo(new[] { 20UL, 21UL });
    }

    [Fact]
    public async Task Reader_Should_Switch_When_Segment_Changes()
    {
        //ToDo this test case is wrong
        var seg1 = new LogSegment("a.log", "a.index", "a.timeindex", 0, 0);
        var seg2 = new LogSegment("b.log", "b.index", "b.timeindex", 100, 100);

        var segReader1 = Substitute.For<ILogSegmentReaderM>();
        var segReader2 = Substitute.For<ILogSegmentReaderM>();

        _registry.GetActiveSegment().Returns(seg1, seg2);
        _segmentFactory.CreateReaderM(seg1).Returns(segReader1);
        _segmentFactory.CreateReaderM(seg2).Returns(segReader2);

        var reader = new BinaryCommitLogReaderM(_segmentFactory, _registry);

        reader.ReadRecordBatch(0);

        reader.ReadRecordBatch(100);

        await segReader1.Received(1).DisposeAsync();
        _segmentFactory.Received(1).CreateReader(seg1);
        _segmentFactory.Received(1).CreateReader(seg2);
    }
}