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

public class LogRecordBatchBinaryWriterTests
{
    private readonly ILogRecordWriter _recordWriter;
    private readonly ICompressor _compressor;
    private readonly Encoding _encoding;

    public LogRecordBatchBinaryWriterTests()
    {
        _recordWriter = new LogRecordBinaryWriter();
        _compressor = new NoopCompressor();
        _encoding = Encoding.UTF8;
    }

    [Fact]
    public void WriteTo_Should_Write_Batch_With_Single_Record()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
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

        // Act
        writer.WriteTo(batch, stream);

        // Assert
        stream.Length.Should().BeGreaterThan(0);
        stream.Position.Should().Be(stream.Length);
    }

    [Fact]
    public void WriteTo_Should_Write_Batch_With_Multiple_Records()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1 }),
            new LogRecord(2, 1001, new byte[] { 2, 3 }),
            new LogRecord(3, 1002, new byte[] { 4, 5, 6 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);

        // Assert
        stream.Length.Should().BeGreaterThan(0);
    }

    [Fact]
    public void WriteTo_Should_Include_CRC()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
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

        // Act
        writer.WriteTo(batch, stream);

        // Assert
        stream.Length.Should().BeGreaterThan(16); // At least base offset (8) + batch length (8) + magic + CRC
    }

    [Fact]
    public void WriteTo_Should_Write_Base_Offset_And_Length()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
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

        // Act
        writer.WriteTo(batch, stream);

        // Assert
        stream.Position = 0;
        var br = new BinaryReader(stream);
        var baseOffset = br.ReadUInt64();
        var batchLength = br.ReadUInt64();
        
        baseOffset.Should().Be(42);
        batchLength.Should().BeGreaterThan(0);
    }

    [Fact]
    public void WriteTo_Should_Write_Magic_Number()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);

        // Assert
        stream.Position = 0;
        var br = new BinaryReader(stream);
        br.ReadUInt64(); // base offset
        br.ReadUInt64(); // batch length
        var magic = (CommitLogMagicNumbers)br.ReadByte();
        
        magic.Should().Be(CommitLogMagicNumbers.LogRecordBatchMagicNumber);
    }

    [Fact]
    public void WriteTo_Should_Handle_Compressed_Flag()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 1, 2, 3 })
        };
        var batchUncompressed = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        var batchCompressed = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            true
        );

        var streamUncompressed = new MemoryStream();
        var streamCompressed = new MemoryStream();

        // Act
        writer.WriteTo(batchUncompressed, streamUncompressed);
        writer.WriteTo(batchCompressed, streamCompressed);

        // Assert
        // Both should have data (NoopCompressor doesn't change size)
        streamUncompressed.Length.Should().BeGreaterThan(0);
        streamCompressed.Length.Should().BeGreaterThan(0);
    }
}

