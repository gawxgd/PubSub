using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Inbound.CommitLog.Record;
using Xunit;

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
        
        readRecord.Offset.Should().Be(42);
        readRecord.Timestamp.Should().Be(5000);
        readRecord.Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1, 2, 3, 4, 5 });
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
        
        readRecord.Offset.Should().Be(10);
        readRecord.Timestamp.Should().Be(2000);
        readRecord.Payload.ToArray().Should().BeEmpty();
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
        
        readRecord.Offset.Should().Be(100);
        readRecord.Timestamp.Should().Be(3000);
        readRecord.Payload.ToArray().Should().BeEquivalentTo(payload);
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
        _writer.WriteTo(record1, bw1, 5000); // Delta = 0
        _writer.WriteTo(record2, bw2, 5000); // Delta = 1000
        bw1.Flush();
        bw2.Flush();

        // Assert - Verify delta encoding efficiency
        stream1.Length.Should().BeLessThan(stream2.Length); // Smaller delta = smaller encoding
        
        // Also verify records can be read back correctly
        stream1.Position = 0;
        stream2.Position = 0;
        var br1 = new BinaryReader(stream1);
        var br2 = new BinaryReader(stream2);
        
        var readRecord1 = _reader.ReadFrom(br1, 5000);
        var readRecord2 = _reader.ReadFrom(br2, 5000);
        
        readRecord1.Offset.Should().Be(1);
        readRecord1.Timestamp.Should().Be(5000);
        readRecord1.Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1 });
        
        readRecord2.Offset.Should().Be(2);
        readRecord2.Timestamp.Should().Be(6000);
        readRecord2.Payload.ToArray().Should().BeEquivalentTo(new byte[] { 2 });
    }
}




