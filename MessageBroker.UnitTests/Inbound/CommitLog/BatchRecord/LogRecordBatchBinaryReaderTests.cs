using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Record;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog.BatchRecord;

public class LogRecordBatchBinaryReaderTests
{
    private readonly ILogRecordWriter _recordWriter;
    private readonly ILogRecordReader _recordReader;
    private readonly ICompressor _compressor;
    private readonly Encoding _encoding;

    public LogRecordBatchBinaryReaderTests()
    {
        _recordWriter = new LogRecordBinaryWriter();
        _recordReader = new LogRecordBinaryReader();
        _compressor = new NoopCompressor();
        _encoding = Encoding.UTF8;
    }

    [Fact]
    public void ReadBatch_Should_Read_Batch_Written_By_Writer()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1, 2, 3 })
        };
        var originalBatch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();
        writer.WriteTo(originalBatch, stream);

        // Act
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.BaseOffset.Should().Be(originalBatch.BaseOffset);
        readBatch.Records.Should().HaveCount(1);
        readBatch.Records.First().Offset.Should().Be(1);
        readBatch.Records.First().Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public void ReadBatch_Should_Read_Multiple_Records()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1 }),
            new LogRecord(2, 1001, new byte[] { 2, 3 }),
            new LogRecord(3, 1002, new byte[] { 4, 5, 6 })
        };
        var originalBatch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();
        writer.WriteTo(originalBatch, stream);

        // Act
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.Should().HaveCount(3);
        readBatch.Records.ElementAt(0).Offset.Should().Be(1);
        readBatch.Records.ElementAt(1).Offset.Should().Be(2);
        readBatch.Records.ElementAt(2).Offset.Should().Be(3);
    }

    [Fact]
    public void ReadBatch_Should_Verify_CRC()
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

        // Corrupt the data (change a byte in the payload area)
        stream.Position = stream.Length - 1;
        stream.WriteByte(99);

        // Act
        stream.Position = 0;
        var act = () => reader.ReadBatch(stream);

        // Assert
        act.Should().Throw<InvalidDataException>()
            .WithMessage("*CRC mismatch*");
    }

    [Fact]
    public void ReadBatch_Should_Handle_Compressed_Batch()
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
            true // compressed
        );
        
        var stream = new MemoryStream();
        writer.WriteTo(batch, stream);

        // Act
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Compressed.Should().BeTrue();
        readBatch.Records.Should().HaveCount(1);
    }

    [Fact]
    public void ReadBatch_Should_Preserve_Base_Offset()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(42, 1000, new byte[] { 1 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            42,
            records,
            false
        );
        
        var stream = new MemoryStream();
        writer.WriteTo(batch, stream);

        // Act
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.BaseOffset.Should().Be(42);
    }
}



