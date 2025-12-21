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
        using var br = new BinaryReader(stream, encoding, true);

        var baseOffset = br.ReadUInt64();
        var batchLength = br.ReadUInt32();
        if (batchLength is 0 or > Int32.MaxValue)
        {
            throw new InvalidDataException("Batch length cannot be zero, or bigger than Int64.MaxValue.");
        }

        var lastOffset = br.ReadUInt64(); // Skip LastOffset, it's computed from records

        var recordBytesLength = br.ReadUInt32();

        var batchStartPosition = stream.Position;

        var magic = (CommitLogMagicNumbers)br.ReadByte();
        if (magic != CommitLogMagicNumbers.LogRecordBatchMagicNumber)
        {
            throw new InvalidDataException(
                $"Invalid magic number: expected {CommitLogMagicNumbers.LogRecordBatchMagicNumber}, got {magic}");
        }

        var storedCrc = br.ReadUInt32();
        var compressedFlag = br.ReadByte();
        var compressed = compressedFlag != 0;

        var baseTimestamp = br.ReadUInt64();

        if (recordBytesLength > int.MaxValue)
        {
            throw new InvalidDataException(
                $"Record bytes length {recordBytesLength} exceeds maximum allowed size");
        }

        var recordBytes = br.ReadBytes((int)recordBytesLength);
        if (recordBytes.Length != (int)recordBytesLength)
        {
            throw new InvalidDataException(
                $"Expected {recordBytesLength} bytes but only read {recordBytes.Length}");
        }

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
        using var br = new BinaryReader(stream, encoding, true);

        var baseOffset = br.ReadUInt64();
        var batchLength = br.ReadUInt32();
        if (batchLength is 0 or > Int32.MaxValue)
        {
            throw new InvalidDataException("Batch length cannot be zero, or bigger than Int32.MaxValue.");
        }

        var lastOffset = br.ReadUInt64();
        var recordBytesLength = br.ReadUInt32();

        // Total batch size = Header (20) + RecordBytesLength (4) + batchLength
        // batchLength already includes: MagicNumber + CRC + CompressedFlag + Timestamp + RecordBytes
        // But RecordBytesLength field (4 bytes) is NOT included in batchLength, so we need to add it
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
        using var recordsReader = new BinaryReader(recordStream, encoding, true);

        while (recordStream.Position < recordStream.Length)
        {
            var record = logRecordReader.ReadFrom(recordsReader, baseTimestamp);
            records.Add(record);
        }

        return records;
    }
}