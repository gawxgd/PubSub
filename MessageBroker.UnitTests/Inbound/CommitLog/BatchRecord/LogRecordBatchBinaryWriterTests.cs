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
    private readonly ILogRecordReader _recordReader;
    private readonly ICompressor _compressor;
    private readonly Encoding _encoding;
    private readonly LogRecordBatchBinaryWriter _batchWriter;
    private readonly LogRecordBatchBinaryReader _batchReader;

    public LogRecordBatchBinaryWriterTests()
    {
        _recordWriter = new LogRecordBinaryWriter();
        _recordReader = new LogRecordBinaryReader();
        _compressor = new NoopCompressor();
        _encoding = Encoding.UTF8;
        _batchWriter = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        _batchReader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
    }

    [Fact]
    public void WriteTo_Should_Write_Batch_With_Single_Record()
    {
        // Arrange
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
        _batchWriter.WriteTo(batch, stream);

        // Assert - Read back and verify
        stream.Position = 0;
        var readBatch = _batchReader.ReadBatch(stream);
        
        readBatch.BaseOffset.Should().Be(0);
        readBatch.Records.Should().HaveCount(1);
        readBatch.Records.ElementAt(0).Offset.Should().Be(1);
        readBatch.Records.ElementAt(0).Timestamp.Should().Be(1000);
        readBatch.Records.ElementAt(0).Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
        readBatch.Compressed.Should().BeFalse();
    }

    [Fact]
    public void WriteTo_Should_Write_Batch_With_Multiple_Records()
    {
        // Arrange
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
        _batchWriter.WriteTo(batch, stream);

        // Assert - Read back and verify
        stream.Position = 0;
        var readBatch = _batchReader.ReadBatch(stream);
        
        readBatch.BaseOffset.Should().Be(0);
        readBatch.Records.Should().HaveCount(3);
        
        readBatch.Records.ElementAt(0).Offset.Should().Be(1);
        readBatch.Records.ElementAt(0).Timestamp.Should().Be(1000);
        readBatch.Records.ElementAt(0).Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1 });
        
        readBatch.Records.ElementAt(1).Offset.Should().Be(2);
        readBatch.Records.ElementAt(1).Timestamp.Should().Be(1001);
        readBatch.Records.ElementAt(1).Payload.ToArray().Should().BeEquivalentTo(new byte[] { 2, 3 });
        
        readBatch.Records.ElementAt(2).Offset.Should().Be(3);
        readBatch.Records.ElementAt(2).Timestamp.Should().Be(1002);
        readBatch.Records.ElementAt(2).Payload.ToArray().Should().BeEquivalentTo(new byte[] { 4, 5, 6 });
    }

    [Fact]
    public void WriteTo_Should_Include_CRC()
    {
        // Arrange
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
        _batchWriter.WriteTo(batch, stream);

        // Assert - Read back and verify CRC is valid (reader will throw if CRC is invalid)
        stream.Position = 0;
        var readBatch = _batchReader.ReadBatch(stream);
        
        readBatch.Should().NotBeNull();
        readBatch.Records.Should().HaveCount(1);
        readBatch.Records.ElementAt(0).Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public void WriteTo_Should_Write_Base_Offset_And_Length()
    {
        // Arrange
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
        _batchWriter.WriteTo(batch, stream);

        // Assert - Read back and verify
        stream.Position = 0;
        var readBatch = _batchReader.ReadBatch(stream);
        
        readBatch.BaseOffset.Should().Be(42);
        readBatch.Records.Should().HaveCount(1);
        readBatch.Records.ElementAt(0).Offset.Should().Be(42);
        readBatch.Records.ElementAt(0).Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1 });
    }

    [Fact]
    public void WriteTo_Should_Write_Magic_Number()
    {
        // Arrange
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
        _batchWriter.WriteTo(batch, stream);

        // Assert - Read back and verify (reader will validate magic number)
        stream.Position = 0;
        var readBatch = _batchReader.ReadBatch(stream);
        
        readBatch.MagicNumber.Should().Be(CommitLogMagicNumbers.LogRecordBatchMagicNumber);
        readBatch.Records.Should().HaveCount(1);
    }

    [Fact]
    public void WriteTo_Should_Handle_Compressed_Flag()
    {
        // Arrange
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
        _batchWriter.WriteTo(batchUncompressed, streamUncompressed);
        _batchWriter.WriteTo(batchCompressed, streamCompressed);

        // Assert - Read back and verify compression flag
        streamUncompressed.Position = 0;
        streamCompressed.Position = 0;
        
        var readBatchUncompressed = _batchReader.ReadBatch(streamUncompressed);
        var readBatchCompressed = _batchReader.ReadBatch(streamCompressed);
        
        readBatchUncompressed.Compressed.Should().BeFalse();
        readBatchUncompressed.Records.ElementAt(0).Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
        
        readBatchCompressed.Compressed.Should().BeTrue();
        readBatchCompressed.Records.ElementAt(0).Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
    }
}




