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

        stream.Position = 24;
        stream.WriteByte(0xFF);

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

        if (stream.Length > 40)
        {
            stream.Position = 40;
            stream.WriteByte(0xFF);
        }

        // Act
        stream.Position = 0;
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
            bw.Write((byte)CommitLogMagicNumbers.LogRecordBatchMagicNumber);
            bw.Write(0u);
            bw.Write(0ul);
            bw.Write((byte)0);
            bw.Write(1000ul);
            bw.WriteVarUInt(0u);
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

        stream.Position = stream.Length - 2;
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
            bw.Write((byte)CommitLogMagicNumbers.LogRecordBatchMagicNumber);
            bw.Write(0u);
            bw.Write(0ul);
            bw.Write((byte)0);
            bw.Write(1000ul);
            bw.WriteVarUInt(1000000u);
        }

        stream.Position = 0;

        // Act
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<Exception>();
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
            true
        );

        var stream = new MemoryStream();
        writer.WriteTo(batch, stream);

        var data = stream.ToArray();
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

