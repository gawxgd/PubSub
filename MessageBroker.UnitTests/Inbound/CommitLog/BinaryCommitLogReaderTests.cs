using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Inbound.CommitLog;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class BinaryCommitLogReaderTests
{
    private readonly ILogSegmentFactory _segmentFactory = Substitute.For<ILogSegmentFactory>();
    private readonly ITopicSegmentManager _manager = Substitute.For<ITopicSegmentManager>();

    public BinaryCommitLogReaderTests()
    {
        AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
    }

    [Fact]
    public void ReadRecords_Should_Return_Whole_Batch_From_Offset()
    {
        var segment = new LogSegment("a.log", "a.index", "a.timeindex", 0, 0);
        _manager.GetActiveSegment().Returns(segment);

        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            10,
            new List<LogRecord>
            {
                new LogRecord(10, 100, new byte[] { 1 }),
                new LogRecord(11, 101, new byte[] { 2 })
            },
            false);

        var segReader = Substitute.For<ILogSegmentReader>();
        segReader.ReadBatch(10).Returns(batch);

        _segmentFactory.CreateReader(segment).Returns(segReader);

        var reader = new BinaryCommitLogReader(_segmentFactory, _manager, "t");

        var records = reader.ReadRecords(10).ToList();

        records.Should().HaveCount(2);
        records.Select(r => r.Offset).Should().BeEquivalentTo(new[] { 10UL, 11UL });
    }

    [Fact]
    public void ReadFromTimestamp_Should_Return_Whole_First_Batch()
    {
        var segment = new LogSegment("a.log", "a.index", "a.timeindex", 0, 0);
        _manager.GetActiveSegment().Returns(segment);

        var firstBatch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            20,
            new List<LogRecord>
            {
                new LogRecord(20, 200, new byte[] { 1 }),
                new LogRecord(21, 201, new byte[] { 2 })
            },
            false);

        var segReader = Substitute.For<ILogSegmentReader>();
        segReader.ReadFromTimestamp(200).Returns(new[] { firstBatch });

        _segmentFactory.CreateReader(segment).Returns(segReader);

        var reader = new BinaryCommitLogReader(_segmentFactory, _manager, "t");

        var records = reader.ReadFromTimestamp(200).ToList();

        records.Should().HaveCount(2);
        records.Select(r => r.Offset).Should().BeEquivalentTo(new[] { 20UL, 21UL });
    }

    [Fact]
    public async Task Reader_Should_Switch_When_Segment_Changes()
    {
        var seg1 = new LogSegment("a.log", "a.index", "a.timeindex", 0, 0);
        var seg2 = new LogSegment("b.log", "b.index", "b.timeindex", 100, 100);

        var segReader1 = Substitute.For<ILogSegmentReader>();
        var segReader2 = Substitute.For<ILogSegmentReader>();

        _manager.GetActiveSegment().Returns(seg1, seg2);
        _segmentFactory.CreateReader(seg1).Returns(segReader1);
        _segmentFactory.CreateReader(seg2).Returns(segReader2);

        var reader = new BinaryCommitLogReader(_segmentFactory, _manager, "t");

        reader.ReadRecords(0).ToList();

        reader.ReadRecords(100).ToList();

        await segReader1.Received(1).DisposeAsync();
        _segmentFactory.Received(1).CreateReader(seg1);
        _segmentFactory.Received(1).CreateReader(seg2);
    }
}


