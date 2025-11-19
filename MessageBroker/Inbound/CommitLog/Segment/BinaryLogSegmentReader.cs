using System.Threading.Channels;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Inbound.CommitLog.BatchRecord;

namespace MessageBroker.Inbound.CommitLog.Segment;

public class BinaryLogSegmentReader: ILogSegmentReader
{
    private LogSegment _segment;
    private readonly LogRecordBatchBinaryReader _batchReader;

    public BinaryLogSegmentReader(LogSegment segment)
    {
        _segment = segment;
        var encoding = System.Text.Encoding.UTF8;
        var compressor = new Compressor.NoopCompressor();
        var logRecordReader = new Record.LogRecordBinaryReader();
        _batchReader = new LogRecordBatchBinaryReader(logRecordReader, compressor, encoding);
    }

    public void ReadAll(Channel<byte[]> channel)
    {
        ulong lastOffset = _segment.BaseOffset;
        
        if (!File.Exists(_segment.LogPath))
        {
            return;
        }

        using var fileStream = new FileStream(
            _segment.LogPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite);

        while (fileStream.Position < fileStream.Length)
        {
            try
            {
                var batch = _batchReader.ReadBatch(fileStream);
                
                foreach (var record in batch.Records)
                {
                    // Convert ReadOnlyMemory<byte> to byte[] for channel
                    var payloadArray = record.Payload.ToArray();
                    channel.Writer.TryWrite(payloadArray);
                    lastOffset = record.Offset;
                }
            }
            catch (Exception)
            {
                // End of file or corrupted data - stop reading
                break;
            }
        }
        
        _segment = _segment with { NextOffset = lastOffset + 1 };
    }

    public LogSegment GetSegment()
    {
        return _segment;
    }

    public void ReadFromOffset(ulong currentOffset, Channel<byte[]> channel)
    {
        ulong lastOffset = currentOffset;
        
        if (!File.Exists(_segment.LogPath))
        {
            return;
        }

        using var fileStream = new FileStream(
            _segment.LogPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite);

        while (fileStream.Position < fileStream.Length)
        {
            try
            {
                var batch = _batchReader.ReadBatch(fileStream);
                
                foreach (var record in batch.Records)
                {
                    if (record.Offset >= currentOffset)
                    {
                        // Convert ReadOnlyMemory<byte> to byte[] for channel
                        var payloadArray = record.Payload.ToArray();
                        channel.Writer.TryWrite(payloadArray);
                        lastOffset = record.Offset;
                    }
                }
            }
            catch (Exception)
            {
                // End of file or corrupted data - stop reading
                break;
            }
        }
        
        _segment = _segment with { NextOffset = lastOffset + 1 };
    }
}