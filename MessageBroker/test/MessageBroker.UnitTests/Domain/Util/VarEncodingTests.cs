using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Util;
using Xunit;

namespace MessageBroker.UnitTests.Domain.Util;

public class VarEncodingTests
{
    [Theory]
    [InlineData(0u, 1)]
    [InlineData(1u, 1)]
    [InlineData(127u, 1)]
    [InlineData(128u, 2)]
    [InlineData(16383u, 2)]
    [InlineData(16384u, 3)]
    [InlineData(2097151u, 3)]
    [InlineData(2097152u, 4)]
    [InlineData(268435455u, 4)]
    [InlineData(268435456u, 5)]
    [InlineData(uint.MaxValue, 5)]
    public void GetVarUIntSize_Should_Calculate_Correct_Size(uint value, int expectedSize)
    {
        // Act
        var size = VarEncodingSize.GetVarUIntSize(value);

        // Assert
        size.Should().Be(expectedSize);
    }

    [Theory]
    [InlineData(0ul, 1)]
    [InlineData(1ul, 1)]
    [InlineData(127ul, 1)]
    [InlineData(128ul, 2)]
    [InlineData(16383ul, 2)]
    [InlineData(16384ul, 3)]
    [InlineData(2097151ul, 3)]
    [InlineData(2097152ul, 4)]
    [InlineData(268435455ul, 4)]
    [InlineData(268435456ul, 5)]
    [InlineData(34359738367ul, 5)]
    [InlineData(34359738368ul, 6)]
    [InlineData(4398046511103ul, 6)]
    [InlineData(4398046511104ul, 7)]
    [InlineData(562949953421311ul, 7)]
    [InlineData(562949953421312ul, 8)]
    [InlineData(72057594037927935ul, 8)]
    [InlineData(72057594037927936ul, 9)]
    [InlineData(ulong.MaxValue, 10)]
    public void GetVarULongSize_Should_Calculate_Correct_Size(ulong value, int expectedSize)
    {
        // Act
        var size = VarEncodingSize.GetVarULongSize(value);

        // Assert
        size.Should().Be(expectedSize);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(1u)]
    [InlineData(127u)]
    [InlineData(128u)]
    [InlineData(255u)]
    [InlineData(256u)]
    [InlineData(16383u)]
    [InlineData(16384u)]
    [InlineData(uint.MaxValue)]
    public void WriteVarUInt_ReadVarUInt_Should_RoundTrip(uint value)
    {
        // Arrange
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, true);
        
        // Act
        writer.WriteVarUInt(value);
        ms.Position = 0;
        
        using var reader = new BinaryReader(ms, Encoding.UTF8);
        var result = reader.ReadVarUInt();

        // Assert
        result.Should().Be(value);
    }

    [Theory]
    [InlineData(0ul)]
    [InlineData(1ul)]
    [InlineData(127ul)]
    [InlineData(128ul)]
    [InlineData(255ul)]
    [InlineData(256ul)]
    [InlineData(16383ul)]
    [InlineData(16384ul)]
    [InlineData(ulong.MaxValue)]
    public void WriteVarULong_ReadVarULong_Should_RoundTrip(ulong value)
    {
        // Arrange
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, true);
        
        // Act
        writer.WriteVarULong(value);
        ms.Position = 0;
        
        using var reader = new BinaryReader(ms, Encoding.UTF8);
        var result = reader.ReadVarULong();

        // Assert
        result.Should().Be(value);
    }

    [Fact]
    public void WriteVarUInt_Should_Use_Expected_Byte_Count()
    {
        // Arrange
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, true);
        uint value = 300;

        // Act
        writer.WriteVarUInt(value);

        // Assert
        ms.Length.Should().Be(2);
        ms.Length.Should().Be(VarEncodingSize.GetVarUIntSize(value));
    }

    [Fact]
    public void WriteVarULong_Should_Use_Expected_Byte_Count()
    {
        // Arrange
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, true);
        ulong value = 300;

        // Act
        writer.WriteVarULong(value);

        // Assert
        ms.Length.Should().Be(2);
        ms.Length.Should().Be(VarEncodingSize.GetVarULongSize(value));
    }

    [Fact]
    public void ReadVarUInt_Should_Throw_On_End_Of_Stream()
    {
        // Arrange
        var ms = new MemoryStream();
        ms.WriteByte(0x80);
        ms.Position = 0;
        using var reader = new BinaryReader(ms);

        // Act
        var act = () => reader.ReadVarUInt();

        // Assert
        act.Should().Throw<EndOfStreamException>();
    }

    [Fact]
    public void ReadVarULong_Should_Throw_On_End_Of_Stream()
    {
        // Arrange
        var ms = new MemoryStream();
        ms.WriteByte(0x80);
        ms.Position = 0;
        using var reader = new BinaryReader(ms);

        // Act
        var act = () => reader.ReadVarULong();

        // Assert
        act.Should().Throw<EndOfStreamException>();
    }

    [Fact]
    public void WriteVarUInt_Should_Match_Size_Calculation()
    {

        for (uint i = 0; i < 1000000; i += 1234)
        {
            var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms, Encoding.UTF8, true);
            
            writer.WriteVarUInt(i);
            
            var actualSize = (int)ms.Length;
            var calculatedSize = VarEncodingSize.GetVarUIntSize(i);
            
            actualSize.Should().Be(calculatedSize, $"value {i} should use {calculatedSize} bytes");
        }
    }

    [Fact]
    public void WriteVarULong_Should_Match_Size_Calculation()
    {

        var testValues = new[] { 0ul, 127ul, 128ul, 16383ul, 16384ul, 
            2097151ul, 2097152ul, 268435455ul, 268435456ul, ulong.MaxValue };
            
        foreach (var value in testValues)
        {
            var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms, Encoding.UTF8, true);
            
            writer.WriteVarULong(value);
            
            var actualSize = (int)ms.Length;
            var calculatedSize = VarEncodingSize.GetVarULongSize(value);
            
            actualSize.Should().Be(calculatedSize, $"value {value} should use {calculatedSize} bytes");
        }
    }
}

