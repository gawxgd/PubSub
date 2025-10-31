using System;
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using FluentAssertions;
using SchemaRegistry.Infrastructure.Validation;
using Xunit;

namespace SchemaRegistry.Tests
{
    /// <summary>
    /// Tests for CompatibilityChecker:
    /// ✅ Backward / Forward / Full compatibility
    /// ✅ SchemaEquals for all Avro schema types
    /// 
    /// TODO: handle type promotion (e.g. int→long)
    /// TODO: handle field name aliases
    /// TODO: handle UnionSchema with reordered members
    /// </summary>
    public class CompatibilityCheckerTests
    {
        private readonly CompatibilityChecker _checker = new();
        private readonly JsonSchemaReader _reader = new();

        private const System.Reflection.BindingFlags PrivateInstance =
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;

        // ======== HELPERS =========

        private RecordSchema ParseRecordSchema(string json)
        {
            var schema = _reader.Read(json);
            schema.Should().BeOfType<RecordSchema>();
            return (RecordSchema)schema;
        }

        private bool SchemaEquals(Schema s1, Schema s2)
        {
            var method = _checker.GetType().GetMethod("SchemaEquals", PrivateInstance);
            method.Should().NotBeNull("SchemaEquals must exist on CompatibilityChecker");
            return (bool)method!.Invoke(_checker, new object[] { s1, s2 })!;
        }

        private static string BaseSchemaJson => """
        {
          "type": "record",
          "name": "User",
          "namespace": "com.example",
          "fields": [
            { "name": "id",     "type": "string" },
            { "name": "age",    "type": "int",    "default": 0 },
            { "name": "active", "type": "boolean","default": true }
          ]
        }
        """;

        // ================================================================
        // SECTION 1: COMPATIBILITY CHECKER (FORWARD / BACKWARD)
        // ================================================================

        [Fact]
        public void Backward_AddingFieldWithDefault_IsCompatible()
        {
            var newSchemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                { "name": "id",     "type": "string" },
                { "name": "age",    "type": "int",    "default": 0 },
                { "name": "active", "type": "boolean","default": true },
                { "name": "country","type": "string","default": "PL" }
              ]
            }
            """;

            var result = _checker.IsBackwardCompatible(ParseRecordSchema(newSchemaJson), ParseRecordSchema(BaseSchemaJson));
            result.Should().BeTrue();
        }

        [Fact]
        public void Backward_AddingFieldWithoutDefault_IsNotCompatible()
        {
            var newSchemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                { "name": "id",     "type": "string" },
                { "name": "age",    "type": "int",    "default": 0 },
                { "name": "active", "type": "boolean","default": true },
                { "name": "country","type": "string" }
              ]
            }
            """;

            var result = _checker.IsBackwardCompatible(ParseRecordSchema(newSchemaJson), ParseRecordSchema(BaseSchemaJson));
            result.Should().BeFalse();
        }

        [Fact]
        public void Backward_DeletingField_IsCompatible()
        {
            var newSchemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                { "name": "id",     "type": "string" },
                { "name": "age",    "type": "int",    "default": 0 }
              ]
            }
            """;

            var result = _checker.IsBackwardCompatible(ParseRecordSchema(newSchemaJson), ParseRecordSchema(BaseSchemaJson));
            result.Should().BeTrue();
        }

        [Fact]
        public void Backward_ChangingExistingFieldType_IsNotCompatible()
        {
            var newSchemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                { "name": "id",     "type": "string" },
                { "name": "age",    "type": "long",   "default": 0 },
                { "name": "active", "type": "boolean","default": true }
              ]
            }
            """;

            var result = _checker.IsBackwardCompatible(ParseRecordSchema(newSchemaJson), ParseRecordSchema(BaseSchemaJson));
            result.Should().BeFalse();
        }

        [Fact]
        public void Forward_DeletingFieldWithDefault_IsCompatible()
        {
            var newSchemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                { "name": "id",  "type": "string" },
                { "name": "age", "type": "int", "default": 0 }
              ]
            }
            """;

            var result = _checker.IsForwardCompatible(ParseRecordSchema(newSchemaJson), ParseRecordSchema(BaseSchemaJson));
            result.Should().BeTrue();
        }

        [Fact]
        public void Forward_DeletingFieldWithoutDefault_IsNotCompatible()
        {
            var newSchemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                { "name": "age",    "type": "int",    "default": 0 },
                { "name": "active", "type": "boolean","default": true }
              ]
            }
            """;

            var result = _checker.IsForwardCompatible(ParseRecordSchema(newSchemaJson), ParseRecordSchema(BaseSchemaJson));
            result.Should().BeFalse();
        }

        [Fact]
        public void Forward_AddingField_IsCompatible()
        {
            var newSchemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                { "name": "id",     "type": "string" },
                { "name": "age",    "type": "int",    "default": 0 },
                { "name": "active", "type": "boolean","default": true },
                { "name": "country","type": "string" }
              ]
            }
            """;

            var result = _checker.IsForwardCompatible(ParseRecordSchema(newSchemaJson), ParseRecordSchema(BaseSchemaJson));
            result.Should().BeTrue();
        }

        [Fact]
        public void Forward_ChangingExistingFieldType_IsNotCompatible()
        {
            var newSchemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                { "name": "id",     "type": "string" },
                { "name": "age",    "type": "long",   "default": 0 },
                { "name": "active", "type": "boolean","default": true }
              ]
            }
            """;

            var result = _checker.IsForwardCompatible(ParseRecordSchema(newSchemaJson), ParseRecordSchema(BaseSchemaJson));
            result.Should().BeFalse();
        }

        // ================================================================
        // SECTION 2: SCHEMA EQUALITY TESTS
        // ================================================================

        [Fact]
        public void SchemaEquals_PrimitiveSchemas_ShouldBeEqual_WhenSameType()
        {
            SchemaEquals(new IntSchema(), new IntSchema()).Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_PrimitiveSchemas_ShouldNotBeEqual_WhenDifferentType()
        {
            SchemaEquals(new IntSchema(), new StringSchema()).Should().BeFalse();
        }

        [Fact]
        public void SchemaEquals_ArraySchemas_ShouldBeEqual_WhenItemsAreEqual()
        {
            SchemaEquals(new ArraySchema(new IntSchema()), new ArraySchema(new IntSchema())).Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_ArraySchemas_ShouldNotBeEqual_WhenItemTypeDiffers()
        {
            SchemaEquals(new ArraySchema(new IntSchema()), new ArraySchema(new StringSchema())).Should().BeFalse();
        }

        [Fact]
        public void SchemaEquals_UnionSchemas_ShouldBeEqual_WhenSameMembers()
        {
            var s1 = new UnionSchema(new Schema[] { new NullSchema(), new StringSchema() });
            var s2 = new UnionSchema(new Schema[] { new NullSchema(), new StringSchema() });
            SchemaEquals(s1, s2).Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_UnionSchemas_ShouldNotBeEqual_WhenDifferentMembers()
        {
            var s1 = new UnionSchema(new Schema[] { new NullSchema(), new StringSchema() });
            var s2 = new UnionSchema(new Schema[] { new IntSchema(), new StringSchema() });
            SchemaEquals(s1, s2).Should().BeFalse();
        }

        [Fact]
        public void SchemaEquals_RecordSchemas_ShouldBeEqual_WhenFullNamesMatch()
        {
            SchemaEquals(new RecordSchema("com.example.User"), new RecordSchema("com.example.User")).Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_RecordSchemas_ShouldNotBeEqual_WhenFullNamesDiffer()
        {
            SchemaEquals(new RecordSchema("com.example.User"), new RecordSchema("com.example.Customer")).Should().BeFalse();
        }

        [Fact]
        public void SchemaEquals_EnumSchemas_ShouldBeEqual_WhenFullNamesMatch()
        {
            SchemaEquals(new EnumSchema("com.example.Status"), new EnumSchema("com.example.Status")).Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_FixedSchemas_ShouldBeEqual_WhenNameAndSizeMatch()
        {
            SchemaEquals(new FixedSchema("com.example.Hash", 16), new FixedSchema("com.example.Hash", 16)).Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_FixedSchemas_ShouldNotBeEqual_WhenSizeDiffers()
        {
            SchemaEquals(new FixedSchema("com.example.Hash", 16), new FixedSchema("com.example.Hash", 32)).Should().BeFalse();
        }
    }
}
