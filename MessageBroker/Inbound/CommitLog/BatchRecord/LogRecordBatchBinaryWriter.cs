using System.Text;
using Force.Crc32;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Util;
using static MessageBroker.Domain.Util.VarEncodingSize;

namespace MessageBroker.Inbound.CommitLog;

public class LogRecordBatchBinaryWriter(ILogRecordWriter recordIo, ICompressor compressor, Encoding encoding)
    : ILogRecordBatchWriter
{
    public void WriteTo(LogRecordBatch recordBatch, Stream stream)
    {
        var recordBytes = WriteRecords(recordBatch.Records, recordBatch.BaseTimestamp);

        if (recordBatch.Compressed)
        {
            recordBytes = compressor.Compress(recordBytes);
        }

        var crc = Crc32Algorithm.Compute(recordBytes);
        var recordBytesLength = (uint)recordBytes.Length;
        var batchLength = GetBatchSize(crc, recordBatch.BaseTimestamp, recordBytesLength);

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

    private ulong GetBatchSize(uint crc, ulong baseTimestamp, uint recordBytesLength)
    {
        return (ulong)(sizeof(byte) // magic number
                       + GetVarUIntSize(crc)
                       + sizeof(byte) // isCompressed 
                       + GetVarULongSize(baseTimestamp)
                       + GetVarUIntSize(recordBytesLength)
                       + sizeof(byte) * recordBytesLength);
    }

    private void WriteHeaders(Stream stream, LogRecordBatch recordBatch, ulong batchLength, uint crc,
        uint recordBytesLength, byte[] recordBytes)
    {
        using var batchRecordWriter = new BinaryWriter(stream, encoding, true);

        batchRecordWriter.Write(recordBatch.BaseOffset);
        batchRecordWriter.Write(batchLength);
        // it is important that baseOffset and batch length is const in size
        batchRecordWriter.Write((byte)recordBatch.MagicNumber);
        batchRecordWriter.WriteVarUInt(crc);
        batchRecordWriter.WriteVarUInt((byte)(recordBatch.Compressed ? 1 : 0));
        batchRecordWriter.WriteVarULong(recordBatch.BaseTimestamp);
        batchRecordWriter.WriteVarUInt(recordBytesLength);
        batchRecordWriter.Write(recordBytes);
    }
}