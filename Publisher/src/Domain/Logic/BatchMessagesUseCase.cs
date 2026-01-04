using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;

namespace Publisher.Domain.Logic;

public sealed class BatchMessagesUseCase(ILogRecordBatchWriter batchWriter)
{
    private readonly List<byte[]> _messages = new(128);
    private int _currentBatchBytes;
    private DateTime _lastFlush = DateTime.UtcNow;

    public int Count => _messages.Count;

    public void Add(byte[] message)
    {
        _messages.Add(message);
        _currentBatchBytes += message.Length;
    }

    public bool ShouldFlush(int maxBytes, TimeSpan maxDelay)
    {
        var now = DateTime.UtcNow;
        var full = _currentBatchBytes >= maxBytes;
        var timeout = now - _lastFlush >= maxDelay;
        return full || timeout;
    }

    public byte[] Build()
    {
        if (_messages.Count == 0)
        {
            throw new InvalidOperationException("Cannot build batch from empty message list");
        }
        
        var timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var records = _messages.Select((msg, index) => new LogRecord(
            Offset: (ulong)index,
            Timestamp: timestamp,
            Payload: msg
        )).ToList();

        var recordBatch = new LogRecordBatch(
            MagicNumber: CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            BaseOffset: 0,
            Records: records,
            Compressed: false
        );

        using var stream = new MemoryStream();
        batchWriter.WriteTo(recordBatch, stream);
        var batchBytes = stream.ToArray();
        
        if (batchBytes == null || batchBytes.Length == 0)
        {
            throw new InvalidOperationException($"Built batch is empty! Had {_messages.Count} messages");
        }
        
        if (batchBytes.Length < 38)
        {
            throw new InvalidOperationException($"Built batch is too small: {batchBytes.Length} bytes (expected at least 38). Had {_messages.Count} messages");
        }
        
        return batchBytes;
    }

    public void Clear()
    {
        _messages.Clear();
        _currentBatchBytes = 0;
        _lastFlush = DateTime.UtcNow;
    }
}
