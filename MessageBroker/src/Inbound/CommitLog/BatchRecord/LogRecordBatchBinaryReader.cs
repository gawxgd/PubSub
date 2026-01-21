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
    private const int HeaderSize = BaseOffsetSize + BatchLengthSize + LastOffsetSize;

    public LogRecordBatch ReadBatch(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[8];

        stream.ReadExactly(buffer[..8]);
        var baseOffset = BinaryPrimitives.ReadUInt64BigEndian(buffer);

        stream.ReadExactly(buffer[..4]);
        var batchLength = BinaryPrimitives.ReadUInt32BigEndian(buffer);
        if (batchLength is 0 or > Int32.MaxValue)
        {
            throw new InvalidDataException("Batch length cannot be zero, or bigger than Int64.MaxValue.");
        }

        stream.ReadExactly(buffer[..8]);
        var lastOffset = BinaryPrimitives.ReadUInt64BigEndian(buffer);

        stream.ReadExactly(buffer[..4]);
        var recordBytesLength = BinaryPrimitives.ReadUInt32BigEndian(buffer);

        var batchStartPosition = stream.Position;

        var magicByte = stream.ReadByte();
        if (magicByte == -1) throw new InvalidDataException("Unexpected end of stream reading magic number");
        var magic = (CommitLogMagicNumbers)magicByte;
        if (magic != CommitLogMagicNumbers.LogRecordBatchMagicNumber)
        {
            throw new InvalidDataException(
                $"Invalid magic number: expected {CommitLogMagicNumbers.LogRecordBatchMagicNumber}, got {magic}");
        }

        stream.ReadExactly(buffer[..4]);
        var storedCrc = BinaryPrimitives.ReadUInt32BigEndian(buffer);

        var compressedByte = stream.ReadByte();
        if (compressedByte == -1) throw new InvalidDataException("Unexpected end of stream reading compressed flag");
        var compressed = compressedByte != 0;

        stream.ReadExactly(buffer[..8]);
        var baseTimestamp = BinaryPrimitives.ReadUInt64BigEndian(buffer);

        if (recordBytesLength > int.MaxValue)
        {
            throw new InvalidDataException(
                $"Record bytes length {recordBytesLength} exceeds maximum allowed size");
        }

        var recordBytes = new byte[recordBytesLength];
        stream.ReadExactly(recordBytes);

        var actualBytesRead = (ulong)(stream.Position - batchStartPosition);
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

    public (byte[] batchBytes, ulong batchOffset, ulong lastOffset) ReadBatchBytesAndAdvance(Stream stream)
    {
        var batchStartPosition = stream.Position;
        Span<byte> buffer = stackalloc byte[8];

        stream.ReadExactly(buffer[..8]);
        var baseOffset = BinaryPrimitives.ReadUInt64BigEndian(buffer);

        stream.ReadExactly(buffer[..4]);
        var batchLength = BinaryPrimitives.ReadUInt32BigEndian(buffer);
        if (batchLength is 0 or > Int32.MaxValue)
        {
            throw new InvalidDataException("Batch length cannot be zero, or bigger than Int32.MaxValue.");
        }

        stream.ReadExactly(buffer[..8]);
        var lastOffset = BinaryPrimitives.ReadUInt64BigEndian(buffer);

        stream.ReadExactly(buffer[..4]);
        var recordBytesLength = BinaryPrimitives.ReadUInt32BigEndian(buffer);

        var totalBatchSize = HeaderSize + sizeof(uint) + (int)batchLength;

        stream.Seek((long)batchStartPosition, SeekOrigin.Begin);

        var fullBatchBytes = new byte[totalBatchSize];
        var bytesRead = stream.Read(fullBatchBytes, 0, totalBatchSize);
        if (bytesRead != totalBatchSize)
        {
            throw new InvalidDataException(
                $"Expected {totalBatchSize} bytes but only read {bytesRead}");
        }

        return (fullBatchBytes, baseOffset, lastOffset);
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
