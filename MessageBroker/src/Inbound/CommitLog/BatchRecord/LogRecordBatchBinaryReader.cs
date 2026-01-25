using System.Buffers.Binary;
using System.Text;
using Force.Crc32;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Util;

namespace MessageBroker.Inbound.CommitLog.BatchRecord;

public class LogRecordBatchBinaryReader(ILogRecordReader logRecordReader, ICompressor compressor, Encoding encoding)
    : ILogRecordBatchReader
{
    private const int BaseOffsetSize = sizeof(ulong);
    private const int BatchLengthSize = sizeof(uint);
    private const int LastOffsetSize = sizeof(ulong);
    private const int RecordBytesLengthSize = sizeof(uint);
    private const int MagicByteSize = sizeof(byte);
    private const int CrcSize = sizeof(uint);
    private const int CompressedFlagSize = sizeof(byte);
    private const int BaseTimestampSize = sizeof(ulong);
    private const int HeaderSize = BaseOffsetSize + BatchLengthSize + LastOffsetSize;

    public LogRecordBatch ReadBatch(ReadOnlySpan<byte> data)
    {
        var position = 0;

        var baseOffset = BinaryPrimitives.ReadUInt64BigEndian(data[position..]);
        position += BaseOffsetSize;

        var batchLength = BinaryPrimitives.ReadUInt32BigEndian(data[position..]);
        position += BatchLengthSize;
        if (batchLength is 0 or > Int32.MaxValue)
        {
            throw new InvalidDataException("Batch length cannot be zero, or bigger than Int64.MaxValue.");
        }

        var lastOffset = BinaryPrimitives.ReadUInt64BigEndian(data[position..]);
        position += LastOffsetSize;

        var recordBytesLength = BinaryPrimitives.ReadUInt32BigEndian(data[position..]);
        position += RecordBytesLengthSize;

        var batchStartPosition = position;

        var magicByte = data[position];
        position += MagicByteSize;
        if (magicByte == 0) throw new InvalidDataException("Unexpected end of data reading magic number");
        var magic = (CommitLogMagicNumbers)magicByte;
        if (magic != CommitLogMagicNumbers.LogRecordBatchMagicNumber)
        {
            throw new InvalidDataException(
                $"Invalid magic number: expected {CommitLogMagicNumbers.LogRecordBatchMagicNumber}, got {magic}");
        }

        var storedCrc = BinaryPrimitives.ReadUInt32BigEndian(data[position..]);
        position += CrcSize;

        var compressedByte = data[position];
        position += CompressedFlagSize;
        var compressed = compressedByte != 0;

        var baseTimestamp = BinaryPrimitives.ReadUInt64BigEndian(data[position..]);
        position += BaseTimestampSize;

        if (recordBytesLength > int.MaxValue)
        {
            throw new InvalidDataException(
                $"Record bytes length {recordBytesLength} exceeds maximum allowed size");
        }

        var recordBytes = data.Slice(position, (int)recordBytesLength).ToArray();
        position += (int)recordBytesLength;

        var actualBytesRead = (ulong)(position - batchStartPosition);
        if (actualBytesRead != batchLength)
        {
            throw new InvalidDataException(
                $"Batch length mismatch: declared {batchLength} bytes but consumed {actualBytesRead} bytes");
        }

        var computedCrc = Crc32Algorithm.Compute(recordBytes);
        if (computedCrc != storedCrc)
        {
            throw new InvalidDataException($"Batch CRC mismatch at offset {baseOffset}");
        }

        if (compressed)
        {
            recordBytes = compressor.Decompress(recordBytes);
        }

        var records = ReadRecords(recordBytes, baseTimestamp);

        return new LogRecordBatch(magic, baseOffset, records, compressed);
    }

    public (byte[] batchBytes, ulong batchOffset, ulong lastOffset, int bytesConsumed) ReadBatchBytesAndAdvance(ReadOnlySpan<byte> data)
    {
        var position = 0;

        var baseOffset = BinaryPrimitives.ReadUInt64BigEndian(data[position..]);
        position += BaseOffsetSize;

        var batchLength = BinaryPrimitives.ReadUInt32BigEndian(data[position..]);
        position += BatchLengthSize;
        if (batchLength is 0 or > Int32.MaxValue)
        {
            throw new InvalidDataException("Batch length cannot be zero, or bigger than Int32.MaxValue.");
        }

        var lastOffset = BinaryPrimitives.ReadUInt64BigEndian(data[position..]);
        position += LastOffsetSize;

        var recordBytesLength = BinaryPrimitives.ReadUInt32BigEndian(data[position..]);
        position += RecordBytesLengthSize;

        var totalBatchSize = HeaderSize + RecordBytesLengthSize + (int)batchLength;

        var fullBatchBytes = data[..totalBatchSize].ToArray();

        return (fullBatchBytes, baseOffset, lastOffset, totalBatchSize);
    }

    private List<LogRecord> ReadRecords(byte[] recordBytes, ulong baseTimestamp)
    {
        var records = new List<LogRecord>();
        using var recordStream = new MemoryStream(recordBytes);

        while (recordStream.Position < recordStream.Length)
        {
            var record = logRecordReader.ReadFrom(recordStream, baseTimestamp);
            records.Add(record);
        }

        return records;
    }
}
