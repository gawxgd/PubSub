using System;
using System.IO;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Avro.IO;
using FluentAssertions;
using Publisher.Domain.Port;
using Publisher.Domain.Service;
using Shared.Domain.Entities.SchemaRegistryClient;
using Xunit;

namespace Publisher.Test;

public sealed class AvroSerializerTests
{

    private sealed record TestMessage
    {
        public string Id { get; init; }
        public int Amount { get; init; }
    }

    private const string TestSchemaJson = """
    {
      "type": "record",
      "name": "TestMessage",
      "namespace": "Publisher.Tests",
      "fields": [
        { "name": "Id", "type": "string" },
        { "name": "Amount", "type": "int" }
      ]
    }
    """;

    private static SchemaInfo CreateSchemaInfo(int schemaId)
        => new SchemaInfo(schemaId, TestSchemaJson, 0);

    [Fact]
    public async Task SerializeAsync_Should_Prefix_Payload_With_SchemaId()
    {
        // arrange
        IAvroSerializer serializer = CreateSerializer();
        var schemaInfo = CreateSchemaInfo(42);
        var message = new TestMessage { Id = "order-1", Amount = 100 };

        // act
        var bytes = await serializer.SerializeAsync(message, schemaInfo);

        // assert
        bytes.Length.Should().BeGreaterThan(sizeof(int));
        BitConverter.ToInt32(bytes, 0).Should().Be(42);
    }

    [Fact]
    public async Task SerializeAsync_Should_Produce_Valid_Avro_Payload()
    {
        // arrange
        IAvroSerializer serializer = CreateSerializer();
        var schemaInfo = CreateSchemaInfo(1);
        var message = new TestMessage { Id = "order-2", Amount = 250 };

        // act
        var bytes = await serializer.SerializeAsync(message, schemaInfo);

        var avroPayload = bytes.AsSpan(sizeof(int)).ToArray();

        var schema = (RecordSchema)Schema.Parse(TestSchemaJson);
        
        using var stream = new MemoryStream(avroPayload);
        var decoder = new BinaryDecoder(stream);
        var reader = new GenericReader<GenericRecord>(schema, schema);
        var deserialized = reader.Read(null, decoder);

        // assert
        deserialized.TryGetValue("Id", out var id);
        deserialized.TryGetValue("Amount", out var amount);
        
        id.Should().Be("order-2");
        amount.Should().Be(250);
    }

    [Fact]
    public async Task SerializeAsync_Should_Encode_Different_SchemaIds_For_Same_Message()
    {
        // arrange
        IAvroSerializer serializer = CreateSerializer();
        var schema1 = CreateSchemaInfo(1);
        var schema2 = CreateSchemaInfo(2);
        var message = new TestMessage { Id = "order-3", Amount = 10 };

        // act
        var bytes1 = await serializer.SerializeAsync(message, schema1);
        var bytes2 = await serializer.SerializeAsync(message, schema2);

        // assert
        BitConverter.ToInt32(bytes1, 0).Should().Be(1);
        BitConverter.ToInt32(bytes2, 0).Should().Be(2);
        bytes1.Should().NotBeEquivalentTo(bytes2);
    }

    [Fact]
    public async Task SerializeAsync_Should_Throw_When_Message_Is_Null()
    {
        // arrange
        IAvroSerializer serializer = CreateSerializer();
        var schemaInfo = CreateSchemaInfo(1);

        // act
        Func<Task> act = () => serializer.SerializeAsync<TestMessage>(null!, schemaInfo);

        // assert
        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    private static IAvroSerializer CreateSerializer()
    {
        return new AvroSerializer();
    }
}
