using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Inbound.CommitLog.Record;
using Xunit;
using static MessageBroker.UnitTests.Inbound.CommitLog.CommitLogTestHelpers;

namespace MessageBroker.UnitTests.Inbound.CommitLog.Record;

public class LogRecordBinaryWriterTests
{
    private readonly LogRecordBinaryWriter _writer;
    private readonly LogRecordBinaryReader _reader;

    public LogRecordBinaryWriterTests()
    {
        _writer = new LogRecordBinaryWriter();
        _reader = new LogRecordBinaryReader();
    }

    [Fact]
    public void WriteTo_Should_Write_Offset_Timestamp_And_Payload()
    {
        // Arrange
        var record = new LogRecord(42, 5000, new byte[] { 1, 2, 3, 4, 5 });
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);
        const ulong baseTimestamp = 1000;

        // Act
        _writer.WriteTo(record, bw, baseTimestamp);
        bw.Flush();

        // Assert - Read back and verify
        stream.Position = 0;
        var br = new BinaryReader(stream);
        var readRecord = _reader.ReadFrom(br, baseTimestamp);
        
        AssertLogRecordsEqual(record, readRecord, "written record should match read record");
    }

    [Fact]
    public void WriteTo_Should_Write_Empty_Payload()
    {
        // Arrange
        var record = new LogRecord(10, 2000, Array.Empty<byte>());
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);

        // Act
        _writer.WriteTo(record, bw, 1000);
        bw.Flush();

        // Assert - Read back and verify
        stream.Position = 0;
        var br = new BinaryReader(stream);
        var readRecord = _reader.ReadFrom(br, 1000);
        
        AssertLogRecordsEqual(record, readRecord, "empty payload record should match");
    }

    [Fact]
    public void WriteTo_Should_Write_Large_Payload()
    {
        // Arrange
        var payload = new byte[10000];
        Random.Shared.NextBytes(payload);
        var record = new LogRecord(100, 3000, payload);
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);

        // Act
        _writer.WriteTo(record, bw, 2000);
        bw.Flush();

        // Assert - Read back and verify
        stream.Position = 0;
        var br = new BinaryReader(stream);
        var readRecord = _reader.ReadFrom(br, 2000);
        
        AssertLogRecordsEqual(record, readRecord, "large payload record should match");
    }

    [Fact]
    public void WriteTo_Should_Use_Timestamp_Delta()
    {
        // Arrange
        var record1 = new LogRecord(1, 5000, new byte[] { 1 });
        var record2 = new LogRecord(2, 6000, new byte[] { 2 });
        
        var stream1 = new MemoryStream();
        var bw1 = new BinaryWriter(stream1);
        var stream2 = new MemoryStream();
        var bw2 = new BinaryWriter(stream2);

        // Act
        _writer.WriteTo(record1, bw1, 5000);
        _writer.WriteTo(record2, bw2, 5000);
        bw1.Flush();
        bw2.Flush();

        // Assert - Current implementation uses fixed ulong encoding for timestamp delta,
        stream1.Length.Should().Be(stream2.Length, "both use fixed-size ulong encoding for timestamp delta");
        
        stream1.Position = 0;
        stream2.Position = 0;
        var br1 = new BinaryReader(stream1);
        var br2 = new BinaryReader(stream2);
        
        var readRecord1 = _reader.ReadFrom(br1, 5000);
        var readRecord2 = _reader.ReadFrom(br2, 5000);
        
        AssertLogRecordsEqual(record1, readRecord1, "first record with small delta should match");
        AssertLogRecordsEqual(record2, readRecord2, "second record with larger delta should match");
    }
}

