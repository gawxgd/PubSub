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
    private readonly string _directory;

    public BinaryCommitLogReader(ILogSegmentFactory segmentFactory, string topic, ulong offset)
    {
        _topic = topic;
        _currentOffset = offset;
        _segmentFactory = segmentFactory;
        _activeSegment = FindFirst(topic, offset);
        _directory = topic;
    }
    public async Task ReadAsync(Channel<byte[]> channel)
    {
        await Task.CompletedTask;
        if (_activeSegment != null)
        {
            var segmentReader = _segmentFactory.CreateReader(_activeSegment);
            segmentReader.ReadFromOffset(_currentOffset, channel);
            _activeSegment = FindNext();
        }

        while (_activeSegment is not null)
        {
            var segmentReader = _segmentFactory.CreateReader(_activeSegment);
            // read all and send to channel 
            segmentReader.ReadAll(channel);
            
            // update offset

            _activeSegment = FindNext();
            // go to next segment
        }
    }

    private LogSegment? FindFirst(string topic, ulong offset)
    {
        if (!Directory.Exists(topic))
        {
            return null;
        }

        var logFiles = Directory.GetFiles(topic, "*.log");
        
        ulong? maxBaseOffset = null;
        string? selectedLogPath = null;

        foreach (var logFile in logFiles)
        {
            var fileName = Path.GetFileNameWithoutExtension(logFile);
            
            if (ulong.TryParse(fileName, out var baseOffset) && baseOffset <= offset)
            {
                if (maxBaseOffset == null || baseOffset > maxBaseOffset.Value)
                {
                    maxBaseOffset = baseOffset;
                    selectedLogPath = logFile;
                }
            }
        }

        if (selectedLogPath == null || maxBaseOffset == null)
        {
            return null;
        }

        var indexPath = Path.ChangeExtension(selectedLogPath, ".index");
        var timeIndexPath = Path.ChangeExtension(selectedLogPath, ".timeindex");
        
        return new LogSegment(selectedLogPath, indexPath, timeIndexPath, maxBaseOffset.Value, maxBaseOffset.Value);
    } 
    private LogSegment? FindNext()
    {
        return null;
    } 
}