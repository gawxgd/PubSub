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
    public LogRecordBatch ReadBatch(Stream stream)
    {
        using var br = new BinaryReader(stream, encoding, true);

        var baseOffset = br.ReadUInt64();
        var batchLength = br.ReadUInt64();
        var magic = (CommitLogMagicNumbers)br.ReadByte();

        var storedCrc = br.ReadVarUInt();
        var compressedFlag = (byte)br.ReadVarUInt();
        var compressed = compressedFlag != 0;

        var baseTimestamp = br.ReadVarULong();
        var recordBytesLength = br.ReadVarUInt();

        var recordBytes = br.ReadBytes((int)recordBytesLength);

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