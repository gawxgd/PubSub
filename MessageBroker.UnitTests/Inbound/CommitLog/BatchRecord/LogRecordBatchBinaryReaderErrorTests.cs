using System.Buffers.Binary;
using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Util;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Record;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog.BatchRecord;

public class LogRecordBatchBinaryReaderErrorTests
{
    private readonly ILogRecordWriter _recordWriter;
    private readonly ILogRecordReader _recordReader;
    private readonly ICompressor _compressor;
    private readonly Encoding _encoding;

    public LogRecordBatchBinaryReaderErrorTests()
    {
        _recordWriter = new LogRecordBinaryWriter();
        _recordReader = new LogRecordBinaryReader();
        _compressor = new NoopCompressor();
        _encoding = Encoding.UTF8;
    }

    [Fact]
    public void ReadBatch_Should_Throw_On_Corrupted_Magic_Number()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);

        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1, 2, 3 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );

        var stream = new MemoryStream();
        writer.WriteTo(batch, stream);

        // Corrupt the magic number (byte 16, after 8-byte baseOffset + 8-byte batchLength)
        stream.Position = 16;
        stream.WriteByte(0xFF); // Invalid magic number

        // Act
        stream.Position = 0;
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<InvalidDataException>()
            .WithMessage("*magic number*");
    }

    [Fact]
    public void ReadBatch_Should_Throw_On_Truncated_Header()
    {
        // Arrange
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        var stream = new MemoryStream();

        // Write partial header (only 10 bytes instead of full header)
        stream.Write(new byte[10]);
        stream.Position = 0;

        // Act
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<EndOfStreamException>();
    }

    [Fact]
    public void ReadBatch_Should_Throw_On_Truncated_Payload()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);

        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1, 2, 3, 4, 5 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );

        var stream = new MemoryStream();
        writer.WriteTo(batch, stream);

        // Truncate the stream by cutting off last 2 bytes
        var truncatedData = stream.ToArray().Take((int)stream.Length - 2).ToArray();
        stream = new MemoryStream(truncatedData);

        // Act
        stream.Position = 0;
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<InvalidDataException>();
    }

    [Fact]
    public void ReadBatch_Should_Throw_On_CRC_Mismatch()
    {
        // Arrange
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        var stream = new MemoryStream();

        // First, write the batch body to calculate its actual size
        var bodyStream = new MemoryStream();
        using (var bodyWriter = new BinaryWriter(bodyStream, _encoding, true))
        {
            bodyWriter.Write((byte)CommitLogMagicNumbers.LogRecordBatchMagicNumber);
            bodyWriter.WriteVarUInt(12345u); // WRONG CRC - intentionally bad
            bodyWriter.WriteVarUInt(0u); // flags (not compressed)
            bodyWriter.WriteVarULong(1000ul); // baseTimestamp
        
            // Write a simple record
            var payload = new byte[] { 1, 2, 3 };
            using var recordMs = new MemoryStream();
            using var recordBw = new BinaryWriter(recordMs, _encoding);
            recordBw.Write(1ul); // offset
            recordBw.WriteVarULong(0ul); // timestamp delta
            recordBw.WriteVarUInt((uint)payload.Length);
            recordBw.Write(payload);
            var recordBytes = recordMs.ToArray();
        
            bodyWriter.WriteVarUInt((uint)recordBytes.Length);
            bodyWriter.Write(recordBytes);
        }
    
        var bodyBytes = bodyStream.ToArray();
        var batchLength = (ulong)bodyBytes.Length;

        // Now write the complete batch with correct length
        using (var bw = new BinaryWriter(stream, _encoding, true))
        {
            bw.Write(0ul); // baseOffset
            bw.Write(batchLength); // Correct batchLength
            bw.Write(bodyBytes); // Write the body
        }

        stream.Position = 0;

        // Act
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<InvalidDataException>()
            .WithMessage("*CRC mismatch*");
    }
    
    [Fact]
    public void ReadBatch_Should_Throw_On_Invalid_Record_Count()
    {
        // Arrange
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        var stream = new MemoryStream();

        using (var bw = new BinaryWriter(stream, _encoding, true))
        {
            // Write a header with invalid data
            bw.Write((byte)CommitLogMagicNumbers.LogRecordBatchMagicNumber);
            bw.Write(0u); // CRC (will be wrong)
            bw.Write(0ul); // baseOffset
            bw.Write((byte)0); // flags (no compression)
            bw.Write(1000ul); // baseTimestamp
            bw.WriteVarUInt(0u); // recordBytesLength - INVALID (0 bytes but claiming records)
        }

        stream.Position = 0;

        // Act
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<InvalidDataException>();
    }

    [Fact]
    public void ReadBatch_Should_Throw_On_Empty_Stream()
    {
        // Arrange
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        var stream = new MemoryStream();

        // Act
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<EndOfStreamException>();
    }

    [Fact]
    public void ReadBatch_Should_Throw_On_Corrupted_Record_Data()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);

        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1, 2, 3, 4, 5 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );

        var stream = new MemoryStream();
        writer.WriteTo(batch, stream);

        // Corrupt a byte in the middle of the record data area
        var midpoint = stream.Length / 2;
        stream.Position = midpoint;
        stream.WriteByte(0xFF);

        // Act
        stream.Position = 0;
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<InvalidDataException>()
            .WithMessage("*CRC mismatch*");
    }

    [Fact]
    public void ReadBatch_Should_Throw_On_Invalid_Payload_Length()
    {
        // Arrange
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        var stream = new MemoryStream();

        using (var bw = new BinaryWriter(stream, _encoding, true))
        {
            // Manually construct a batch with invalid payload length
            bw.Write((byte)CommitLogMagicNumbers.LogRecordBatchMagicNumber);
            bw.Write(0u); // CRC placeholder
            bw.Write(0ul); // baseOffset
            bw.Write((byte)0); // flags
            bw.Write(1000ul); // baseTimestamp
            bw.WriteVarUInt(1000000u); // Claim 1MB of record data but don't provide it
        }

        stream.Position = 0;

        // Act
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<Exception>(); // Will throw due to unable to read that much data
    }

    [Fact]
    public void ReadBatch_Should_Handle_Multiple_CRC_Corruptions()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);

        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1, 2 }),
            new LogRecord(2, 1001, new byte[] { 3, 4 }),
            new LogRecord(3, 1002, new byte[] { 5, 6 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );

        var stream = new MemoryStream();
        writer.WriteTo(batch, stream);

        // Corrupt multiple bytes in payload
        stream.Position = stream.Length - 5;
        stream.Write(new byte[] { 0xFF, 0xFF, 0xFF });

        // Act
        stream.Position = 0;
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<InvalidDataException>()
            .WithMessage("*CRC mismatch*");
    }

    [Fact]
    public void ReadBatch_Should_Throw_On_Invalid_Compression_Flag()
    {
        // This test ensures that if compression is indicated but compressor fails,
        // the error is propagated correctly

        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);

        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1, 2, 3 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            true // compressed
        );

        var stream = new MemoryStream();
        writer.WriteTo(batch, stream);

        // Corrupt the compressed data portion
        var data = stream.ToArray();
        // Find and corrupt the payload after headers
        for (int i = 30; i < Math.Min(data.Length, 40); i++)
        {
            data[i] = 0xFF;
        }

        stream = new MemoryStream(data);

        // Act
        stream.Position = 0;
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<InvalidDataException>()
            .WithMessage("*CRC mismatch*");
    }
}