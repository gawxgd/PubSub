using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Record;
using Xunit;
using static MessageBroker.UnitTests.Inbound.CommitLog.CommitLogTestHelpers;

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
        
        AssertBatchesEqual(batch, readBatch, "single record batch should match");
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
        
        AssertBatchesEqual(batch, readBatch, "multiple records batch should match");
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
        
        AssertBatchesEqual(batch, readBatch, "batch with CRC should match");
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
        
        AssertBatchesEqual(batch, readBatch, "batch with base offset 42 should match");
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
        
        AssertBatchesEqual(batch, readBatch, "batch with magic number should match");
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
        
        AssertBatchesEqual(batchUncompressed, readBatchUncompressed, "uncompressed batch should match");
        AssertBatchesEqual(batchCompressed, readBatchCompressed, "compressed batch should match");
    }
}




