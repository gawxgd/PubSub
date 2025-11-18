using System.Threading.Channels;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog;

public class BinaryCommitLogReader: ICommitLogReader
{
    private LogSegment? _activeSegment;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly string _topic;
    private ulong _currentOffset;

    public BinaryCommitLogReader(ILogSegmentFactory segmentFactory, string topic, ulong offset)
    {
        _topic = topic;
        _currentOffset = offset;
        _segmentFactory = segmentFactory;
        _activeSegment = FindFirst();
    }
    public async Task ReadAsync(Channel<byte[]> channel)
    {
        await Task.CompletedTask;

        while (_activeSegment is not null)
        {
            var segmentReader = _segmentFactory.CreateReader(_activeSegment);
            // read all and send to channel 
            
            // update offset

            _activeSegment = FindNext();
            // go to next segment
        }
    }

    private LogSegment? FindFirst()
    {
        return null;
    } 
    private LogSegment? FindNext()
    {
        return null;
    } 
}