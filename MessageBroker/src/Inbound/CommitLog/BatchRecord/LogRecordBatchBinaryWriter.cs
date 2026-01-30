using System.Text;
using Force.Crc32;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Util;
using static MessageBroker.Domain.Util.VarEncodingSize;

namespace MessageBroker.Inbound.CommitLog.BatchRecord;

public class LogRecordBatchBinaryWriter(ILogRecordWriter recordIo, ICompressor compressor, Encoding encoding)
    : ILogRecordBatchWriter
{
    private const int MagicNumberSize = sizeof(byte);
    private const int CrcSize = sizeof(uint);
    private const int CompressedFlagSize = sizeof(byte);
    private const int TimestampSize = sizeof(ulong);
    private const int RecordPayloadLengthSize = sizeof(uint);

    public void WriteTo(LogRecordBatch recordBatch, Stream stream)
    {
        var recordBytes = WriteRecords(recordBatch.Records, recordBatch.BaseTimestamp);

        if (recordBatch.Compressed)
        {
            recordBytes = compressor.Compress(recordBytes);
        }

        var crc = Crc32Algorithm.Compute(recordBytes);
        var recordBytesLength = (uint)recordBytes.Length;
        var batchLength = GetBatchSize(recordBytesLength);

        WriteHeaders(stream, recordBatch, batchLength, crc, recordBytesLength, recordBytes);
    }

    private byte[] WriteRecords(ICollection<LogRecord> records, ulong baseTimestamp)
    {
        using var recordsStream = new MemoryStream();
        using var recordWriter = new BinaryWriter(recordsStream, encoding, false);

        foreach (var record in records)
        {
            recordIo.WriteTo(record, recordWriter, baseTimestamp);
        }

        return recordsStream.ToArray();
    }

    private uint GetBatchSize(uint recordBytesLength)
    {
        return MagicNumberSize +
               CrcSize
               + CompressedFlagSize
               + TimestampSize
               + sizeof(byte) * recordBytesLength;
    }

    private void WriteHeaders(Stream stream, LogRecordBatch recordBatch, uint batchLength, uint crc,
        uint recordBytesLength, byte[] recordBytes)
    {
        using var batchRecordWriter = new BinaryWriter(stream, encoding, true);

        batchRecordWriter.Write((ulong)recordBatch.BaseOffset);
        batchRecordWriter.Write((uint)batchLength);
        batchRecordWriter.Write((ulong)recordBatch.LastOffset);
        batchRecordWriter.Write((uint)recordBytesLength);
        batchRecordWriter.Write((byte)recordBatch.MagicNumber);
        batchRecordWriter.Write((uint)crc);
        batchRecordWriter.Write((byte)(recordBatch.Compressed ? 1 : 0));
        batchRecordWriter.Write((ulong)recordBatch.BaseTimestamp);
        batchRecordWriter.Write(recordBytes);
    }
}
