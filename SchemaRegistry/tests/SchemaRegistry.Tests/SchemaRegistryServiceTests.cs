using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Moq;
using SchemaRegistry.Domain.Enums;
using SchemaRegistry.Domain.Exceptions;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Domain.Port;
using SchemaRegistry.Infrastructure.Adapter;
using SchemaRegistry.Infrastructure.Validation;
using Xunit;

namespace SchemaRegistry.Tests
{
    public class SchemaRegistryServiceTests
    {
        private readonly Mock<ISchemaStore> _store = new();
        private readonly Mock<ICompatibilityChecker> _checker = new();
        private readonly Mock<ISchemaCompatibilityService> _compatibility = new();
        private readonly IConfiguration _cfg;

        private readonly SchemaRegistryService _service;

        private const string Topic = "users";
        private const string ValidSchemaJson = """
        {
          "type": "record",
          "name": "User",
          "namespace": "com.example",
          "fields": [
            {"name": "id", "type": "string"}
          ]
        }
        """;

        public SchemaRegistryServiceTests()
        {
            var inMemorySettings = new Dictionary<string, string?>
            {
                { "SchemaRegistry:CompatibilityMode", "BACKWARD" }
            };
            _cfg = new ConfigurationBuilder().AddInMemoryCollection(inMemorySettings).Build();
            
            _compatibility.Setup(c => c.IsCompatible(It.IsAny<string>(), It.IsAny<string>(), CompatibilityMode.Backward))
                .Returns(false);

            _service = new SchemaRegistryService(_store.Object, _compatibility.Object, _cfg);
        }

        // ============================================================
        // BASIC VALIDATION
        // ============================================================

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task RegisterSchemaAsync_ShouldThrow_WhenTopicInvalid(string topic)
        {
            Func<Task> act = () => _service.RegisterSchemaAsync(topic, ValidSchemaJson);
            await act.Should().ThrowAsync<ArgumentException>().WithMessage("*topic*");
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task RegisterSchemaAsync_ShouldThrow_WhenSchemaInvalid(string schema)
        {
            Func<Task> act = () => _service.RegisterSchemaAsync("test", schema);
            await act.Should().ThrowAsync<ArgumentException>().WithMessage("*schemaJson*");
        }

        // ============================================================
        // DEDUPLICATION
        // ============================================================

        [Fact]
        public async Task RegisterSchemaAsync_ShouldReturnExistingId_WhenChecksumMatches()
        {
            // Arrange
            var existing = new SchemaEntity { Id = 42, Topic = Topic, SchemaJson = ValidSchemaJson, Checksum = "abc" };
            _store.Setup(s => s.GetByChecksumAsync(It.IsAny<string>())).ReturnsAsync(existing);

            // Act
            var id = await _service.RegisterSchemaAsync(Topic, ValidSchemaJson);

            // Assert
            id.Should().Be(42);
            _store.Verify(s => s.SaveAsync(It.IsAny<SchemaEntity>()), Times.Never);
        }

        // ============================================================
        // COMPATIBILITY VALIDATION
        // ============================================================

        [Fact]
        public async Task RegisterSchemaAsync_ShouldThrow_WhenBackwardIncompatible()
        {
            // Arrange
            var latest = new SchemaEntity { Id = 1, Topic = Topic, SchemaJson = ValidSchemaJson };
            _store.Setup(s => s.GetByChecksumAsync(It.IsAny<string>())).ReturnsAsync((SchemaEntity?)null);
            _store.Setup(s => s.GetLatestForTopicAsync(Topic)).ReturnsAsync(latest);
            _checker.Setup(c => c.IsBackwardCompatible(It.IsAny<RecordSchema>(), It.IsAny<RecordSchema>()))
                    .Returns(false);

            // Act
            Func<Task> act = () => _service.RegisterSchemaAsync(Topic, ValidSchemaJson);

            // Assert
            await act.Should().ThrowAsync<SchemaCompatibilityException>()
                .WithMessage("New schema is not Backward-compatible with latest for topic.");
            _store.Verify(s => s.SaveAsync(It.IsAny<SchemaEntity>()), Times.Never);
        }

        [Fact]
        public async Task RegisterSchemaAsync_ShouldSave_WhenBackwardCompatible()
        {
            // Arrange
            var latest = new SchemaEntity { Id = 1, Topic = Topic, SchemaJson = ValidSchemaJson };
            _store.Setup(s => s.GetByChecksumAsync(It.IsAny<string>())).ReturnsAsync((SchemaEntity?)null);
            _store.Setup(s => s.GetLatestForTopicAsync(Topic)).ReturnsAsync(latest);

            var compatMock = new Mock<ISchemaCompatibilityService>();
            compatMock.Setup(c => c.IsCompatible(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CompatibilityMode>()))
                .Returns(true);

            var cfg = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?> { ["SchemaRegistry:CompatibilityMode"] = "BACKWARD" })
                .Build();

            var service = new SchemaRegistryService(_store.Object, compatMock.Object, cfg);

            _store.Setup(s => s.SaveAsync(It.IsAny<SchemaEntity>()))
                .ReturnsAsync((SchemaEntity e) => { e.Id = 100; return e; });

            // Act
            var id = await service.RegisterSchemaAsync(Topic, ValidSchemaJson);

            // Assert
            id.Should().Be(100);
            _store.Verify(s => s.SaveAsync(It.IsAny<SchemaEntity>()), Times.Once);
        }


        [Fact]
        public async Task RegisterSchemaAsync_ShouldSave_WhenNoPreviousSchema()
        {
            // Arrange
            _store.Setup(s => s.GetByChecksumAsync(It.IsAny<string>())).ReturnsAsync((SchemaEntity?)null);
            _store.Setup(s => s.GetLatestForTopicAsync(Topic)).ReturnsAsync((SchemaEntity?)null);
            _store.Setup(s => s.SaveAsync(It.IsAny<SchemaEntity>()))
                .ReturnsAsync((SchemaEntity e) => { e.Id = 1; return e; });

            // Act
            var id = await _service.RegisterSchemaAsync(Topic, ValidSchemaJson);

            // Assert
            id.Should().Be(1);
            _store.Verify(s => s.SaveAsync(It.IsAny<SchemaEntity>()), Times.Once);
        }

        // ============================================================
        // COMPATIBILITY MODES
        // ============================================================

        [Theory]
        [InlineData("BACKWARD", CompatibilityMode.Backward)]
        [InlineData("FORWARD", CompatibilityMode.Forward)]
        [InlineData("FULL", CompatibilityMode.Full)]
        [InlineData("NONE", CompatibilityMode.None)]
        public async Task RegisterSchemaAsync_ShouldUseProperCompatibilityMode(string modeString, CompatibilityMode expectedMode)
        {
            // Arrange
            var cfg = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?> { ["SchemaRegistry:CompatibilityMode"] = modeString })
                .Build();

            var service = new SchemaRegistryService(_store.Object, _compatibility.Object, cfg);

            var newSchema = """{ "type": "record", "name": "User", "fields": [ { "name": "id", "type": "string" } ] }""";
            var oldSchema = newSchema;

            _store.Setup(s => s.GetByChecksumAsync(It.IsAny<string>())).ReturnsAsync((SchemaEntity?)null);
            _store.Setup(s => s.GetLatestForTopicAsync(Topic)).ReturnsAsync(new SchemaEntity { SchemaJson = oldSchema });
            _compatibility.Setup(c => c.IsCompatible(oldSchema, newSchema, expectedMode)).Returns(true);
            _store.Setup(s => s.SaveAsync(It.IsAny<SchemaEntity>())).ReturnsAsync(new SchemaEntity { Id = 1 });

            // Act
            var id = await service.RegisterSchemaAsync(Topic, newSchema);

            // Assert
            id.Should().Be(1);
            _compatibility.Verify(c => c.IsCompatible(oldSchema, newSchema, expectedMode), Times.Once);
        }


        // ============================================================
        // SIMPLE GETTERS
        // ============================================================

        [Fact]
        public async Task GetLatestSchemaAsync_ShouldDelegateToStore()
        {
            var entity = new SchemaEntity { Id = 99, Topic = Topic };
            _store.Setup(s => s.GetLatestForTopicAsync(Topic)).ReturnsAsync(entity);

            var result = await _service.GetLatestSchemaAsync(Topic);

            result.Should().BeSameAs(entity);
            _store.Verify(s => s.GetLatestForTopicAsync(Topic), Times.Once);
        }

        [Fact]
        public async Task GetSchemaByIdAsync_ShouldDelegateToStore()
        {
            var entity = new SchemaEntity { Id = 12 };
            _store.Setup(s => s.GetByIdAsync(12)).ReturnsAsync(entity);

            var result = await _service.GetSchemaByIdAsync(12);

            result.Should().BeSameAs(entity);
            _store.Verify(s => s.GetByIdAsync(12), Times.Once);
        }

        [Fact]
        public async Task GetVersionsAsync_ShouldDelegateToStore()
        {
            var list = new[] { new SchemaEntity { Id = 1 }, new SchemaEntity { Id = 2 } };
            _store.Setup(s => s.GetAllForTopicAsync(Topic)).ReturnsAsync(list);

            var result = await _service.GetVersionsAsync(Topic);

            result.Should().BeEquivalentTo(list);
        }
    }
}
