using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Inbound.CommitLog.Record;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog.Record;

public class LogRecordBinaryWriterTests
{
    [Fact]
    public void WriteTo_Should_Write_Offset_Timestamp_And_Payload()
    {
        // Arrange
        var writer = new LogRecordBinaryWriter();
        var record = new LogRecord(42, 5000, new byte[] { 1, 2, 3, 4, 5 });
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);
        const ulong baseTimestamp = 1000;

        // Act
        writer.WriteTo(record, bw, baseTimestamp);
        bw.Flush();

        // Assert
        stream.Position = 0;
        var br = new BinaryReader(stream);
        var offset = br.ReadUInt64();
        offset.Should().Be(42);
        
        // Timestamp is stored as delta
        stream.Length.Should().BeGreaterThan(8); // More than just offset
    }

    [Fact]
    public void WriteTo_Should_Write_Empty_Payload()
    {
        // Arrange
        var writer = new LogRecordBinaryWriter();
        var record = new LogRecord(10, 2000, Array.Empty<byte>());
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);

        // Act
        writer.WriteTo(record, bw, 1000);
        bw.Flush();

        // Assert
        // ToDo use my reader and assert all values
        
        stream.Length.Should().BeGreaterThan(0);
    }

    [Fact]
    public void WriteTo_Should_Write_Large_Payload()
    {
        // Arrange
        var writer = new LogRecordBinaryWriter();
        var payload = new byte[10000];
        Random.Shared.NextBytes(payload);
        var record = new LogRecord(100, 3000, payload);
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);

        // Act
        writer.WriteTo(record, bw, 2000);
        bw.Flush();

        // Assert
        stream.Length.Should().BeGreaterThan(10000); // Payload + metadata
    }

    [Fact]
    public void WriteTo_Should_Use_Timestamp_Delta()
    {
        // Arrange
        var writer = new LogRecordBinaryWriter();
        var record1 = new LogRecord(1, 5000, new byte[] { 1 });
        var record2 = new LogRecord(2, 6000, new byte[] { 2 });
        
        var stream1 = new MemoryStream();
        var bw1 = new BinaryWriter(stream1);
        var stream2 = new MemoryStream();
        var bw2 = new BinaryWriter(stream2);

        // Act
        writer.WriteTo(record1, bw1, 5000); // Delta = 0
        writer.WriteTo(record2, bw2, 5000); // Delta = 1000
        bw1.Flush();
        bw2.Flush();

        // Assert
        stream1.Length.Should().BeLessThan(stream2.Length); // Smaller delta = smaller encoding
    }
}

