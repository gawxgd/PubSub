using System;
using System.Linq;
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

        // ======== HELPERS =========
        private RecordSchema ParseRecord(string json)
        {
            var schema = _reader.Read(json);
            schema.Should().BeOfType<RecordSchema>();
            return (RecordSchema)schema;
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

            var result = _checker.IsBackwardCompatible(ParseRecord(newSchemaJson), ParseRecord(BaseSchemaJson));
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

            var result = _checker.IsBackwardCompatible(ParseRecord(newSchemaJson), ParseRecord(BaseSchemaJson));
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

            var result = _checker.IsBackwardCompatible(ParseRecord(newSchemaJson), ParseRecord(BaseSchemaJson));
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

            var result = _checker.IsBackwardCompatible(ParseRecord(newSchemaJson), ParseRecord(BaseSchemaJson));
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

            var result = _checker.IsForwardCompatible(ParseRecord(newSchemaJson), ParseRecord(BaseSchemaJson));
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

            var result = _checker.IsForwardCompatible(ParseRecord(newSchemaJson), ParseRecord(BaseSchemaJson));
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

            var result = _checker.IsForwardCompatible(ParseRecord(newSchemaJson), ParseRecord(BaseSchemaJson));
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

            var result = _checker.IsForwardCompatible(ParseRecord(newSchemaJson), ParseRecord(BaseSchemaJson));
            result.Should().BeFalse();
        }

        // ================================================================
        // SECTION 2: SCHEMA EQUALITY TESTS
        // ================================================================

        [Fact]
        public void SchemaEquals_PrimitiveSchemas_ShouldBeEqual_WhenSameType()
        {
            var s1 = new IntSchema();
            var s2 = new IntSchema();
            _checker.GetType().GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })
                .Should().Be(true);
        }

        [Fact]
        public void SchemaEquals_PrimitiveSchemas_ShouldNotBeEqual_WhenDifferentType()
        {
            var s1 = new IntSchema();
            var s2 = new StringSchema();
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeFalse();
        }

        [Fact]
        public void SchemaEquals_ArraySchemas_ShouldBeEqual_WhenItemsAreEqual()
        {
            var s1 = new ArraySchema(new IntSchema());
            var s2 = new ArraySchema(new IntSchema());
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_ArraySchemas_ShouldNotBeEqual_WhenItemTypeDiffers()
        {
            var s1 = new ArraySchema(new IntSchema());
            var s2 = new ArraySchema(new StringSchema());
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeFalse();
        }

        [Fact]
        public void SchemaEquals_UnionSchemas_ShouldBeEqual_WhenSameMembers()
        {
            var s1 = new UnionSchema(new Schema[] { new NullSchema(), new StringSchema() });
            var s2 = new UnionSchema(new Schema[] { new NullSchema(), new StringSchema() });
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_UnionSchemas_ShouldNotBeEqual_WhenDifferentMembers()
        {
            var s1 = new UnionSchema(new Schema[] { new NullSchema(), new StringSchema() });
            var s2 = new UnionSchema(new Schema[] { new IntSchema(), new StringSchema() });
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeFalse();
        }

        [Fact]
        public void SchemaEquals_RecordSchemas_ShouldBeEqual_WhenFullNamesMatch()
        {
            var s1 = new RecordSchema("com.example.User");
            var s2 = new RecordSchema("com.example.User");
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_RecordSchemas_ShouldNotBeEqual_WhenFullNamesDiffer()
        {
            var s1 = new RecordSchema("com.example.User");
            var s2 = new RecordSchema("com.example.Customer");
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeFalse();
        }

        [Fact]
        public void SchemaEquals_EnumSchemas_ShouldBeEqual_WhenFullNamesMatch()
        {
            var s1 = new EnumSchema("com.example.Status");
            var s2 = new EnumSchema("com.example.Status");
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_FixedSchemas_ShouldBeEqual_WhenNameAndSizeMatch()
        {
            var s1 = new FixedSchema("com.example.Hash", 16);
            var s2 = new FixedSchema("com.example.Hash", 16);
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeTrue();
        }

        [Fact]
        public void SchemaEquals_FixedSchemas_ShouldNotBeEqual_WhenSizeDiffers()
        {
            var s1 = new FixedSchema("com.example.Hash", 16);
            var s2 = new FixedSchema("com.example.Hash", 32);
            var result = (bool)_checker.GetType()
                .GetMethod("SchemaEquals", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(_checker, new object[] { s1, s2 })!;
            result.Should().BeFalse();
        }
    }
}
