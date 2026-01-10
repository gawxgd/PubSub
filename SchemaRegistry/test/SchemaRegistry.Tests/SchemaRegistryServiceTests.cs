using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Chr.Avro.Abstract;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Moq;
using SchemaRegistry.Domain.Enums;
using SchemaRegistry.Domain.Exceptions;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Domain.Port;
using SchemaRegistry.Infrastructure.Adapter;
using Xunit;

namespace SchemaRegistry.Tests
{
    public class SchemaRegistryServiceTests
    {
        private Mock<ISchemaStore> _store = null!;
        private Mock<ISchemaCompatibilityService> _compatibility = null!;
        private IConfiguration _cfg = null!;
        private SchemaRegistryService _service = null!;

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
            ResetMocks();
        }

        /// <summary>
        /// Przywraca czysty stan mocków i serwisu.
        /// </summary>
        private void ResetMocks()
        {
            _store = new Mock<ISchemaStore>(MockBehavior.Strict);
            _compatibility = new Mock<ISchemaCompatibilityService>(MockBehavior.Strict);

            var inMemorySettings = new Dictionary<string, string?>
            {
                { "SchemaRegistry:CompatibilityMode", "BACKWARD" }
            };
            _cfg = new ConfigurationBuilder().AddInMemoryCollection(inMemorySettings).Build();

            _compatibility
                .Setup(c => c.IsCompatible(It.IsAny<string>(), It.IsAny<string>(), CompatibilityMode.Backward))
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
            // Arrange
            ResetMocks();

            // Act
            Func<Task> act = () => _service.RegisterSchemaAsync(topic, ValidSchemaJson);

            // Assert
            await act.Should().ThrowAsync<ArgumentException>().WithMessage("*topic*");
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task RegisterSchemaAsync_ShouldThrow_WhenSchemaInvalid(string schema)
        {
            // Arrange
            ResetMocks();

            // Act
            Func<Task> act = () => _service.RegisterSchemaAsync("test", schema);

            // Assert
            await act.Should().ThrowAsync<ArgumentException>().WithMessage("*schemaJson*");
        }

        // ============================================================
        // DEDUPLICATION
        // ============================================================

        [Fact]
        public async Task RegisterSchemaAsync_ShouldReturnExistingId_WhenChecksumMatches()
        {
            // Arrange
            ResetMocks();

            string? savedChecksum = null;

            _store.SetupSequence(s => s.GetLatestForTopicAsync(Topic))
                .ReturnsAsync((SchemaEntity?)null) // 1st call: no previous schema -> will Save
                .ReturnsAsync(() => new SchemaEntity // 2nd call: latest has checksum from first save
                {
                    Id = 42,
                    Topic = Topic,
                    SchemaJson = ValidSchemaJson,
                    Checksum = savedChecksum!
                });

            _store.Setup(s => s.SaveAsync(It.IsAny<SchemaEntity>()))
                .ReturnsAsync((SchemaEntity e) =>
                {
                    savedChecksum = e.Checksum; // checksum computed by production code
                    e.Id = 42;
                    return e;
                });

            // Act
            var id1 = await _service.RegisterSchemaAsync(Topic, ValidSchemaJson);
            var id2 = await _service.RegisterSchemaAsync(Topic, ValidSchemaJson);

            // Assert
            id1.Should().Be(42);
            id2.Should().Be(42);
            _store.Verify(s => s.SaveAsync(It.IsAny<SchemaEntity>()), Times.Once);
        }

        [Fact]
        public async Task RegisterSchemaAsync_ShouldNotDeduplicateAcrossTopics_WhenChecksumMatches()
        {
            // Arrange
            ResetMocks();

            var otherTopic = "orders";
            string? checksumFromOtherTopic = null;
            string? checksumFromTopic = null;

            _store.Setup(s => s.GetLatestForTopicAsync(otherTopic)).ReturnsAsync((SchemaEntity?)null);
            _store.Setup(s => s.SaveAsync(It.Is<SchemaEntity>(e => e.Topic == otherTopic)))
                .ReturnsAsync((SchemaEntity e) =>
                {
                    checksumFromOtherTopic = e.Checksum;
                    e.Id = 10;
                    return e;
                });

            _store.Setup(s => s.GetLatestForTopicAsync(Topic)).ReturnsAsync((SchemaEntity?)null);
            _store.Setup(s => s.SaveAsync(It.Is<SchemaEntity>(e => e.Topic == Topic)))
                .ReturnsAsync((SchemaEntity e) =>
                {
                    checksumFromTopic = e.Checksum;
                    e.Id = 100;
                    return e;
                });

            // Act
            var idOther = await _service.RegisterSchemaAsync(otherTopic, ValidSchemaJson);
            var idTopic = await _service.RegisterSchemaAsync(Topic, ValidSchemaJson);

            // Assert
            checksumFromOtherTopic.Should().NotBeNullOrEmpty();
            checksumFromTopic.Should().NotBeNullOrEmpty();
            checksumFromTopic.Should().Be(checksumFromOtherTopic); // same schema => same checksum

            idOther.Should().Be(10);
            idTopic.Should().Be(100);
            _store.Verify(s => s.SaveAsync(It.IsAny<SchemaEntity>()), Times.Exactly(2));
        }

        // ============================================================
        // COMPATIBILITY VALIDATION
        // ============================================================

        [Fact]
        public async Task RegisterSchemaAsync_ShouldThrow_WhenBackwardIncompatible()
        {
            // Arrange
            ResetMocks();

            var latest = new SchemaEntity { Id = 1, Topic = Topic, SchemaJson = ValidSchemaJson };
            _store.Setup(s => s.GetByChecksumAsync(It.IsAny<string>())).ReturnsAsync((SchemaEntity?)null);
            _store.Setup(s => s.GetLatestForTopicAsync(Topic)).ReturnsAsync(latest);
            _compatibility.Setup(c => c.IsCompatible(It.IsAny<string>(), It.IsAny<string>(), CompatibilityMode.Backward))
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
            ResetMocks();

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
            ResetMocks();

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
            ResetMocks();

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
            // Arrange
            ResetMocks();

            var entity = new SchemaEntity { Id = 99, Topic = Topic };
            _store.Setup(s => s.GetLatestForTopicAsync(Topic)).ReturnsAsync(entity);

            // Act
            var result = await _service.GetLatestSchemaAsync(Topic);

            // Assert
            result.Should().BeSameAs(entity);
            _store.Verify(s => s.GetLatestForTopicAsync(Topic), Times.Once);
        }

        [Fact]
        public async Task GetSchemaByIdAsync_ShouldDelegateToStore()
        {
            // Arrange
            ResetMocks();

            var entity = new SchemaEntity { Id = 12 };
            _store.Setup(s => s.GetByIdAsync(12)).ReturnsAsync(entity);

            // Act
            var result = await _service.GetSchemaByIdAsync(12);

            // Assert
            result.Should().BeSameAs(entity);
            _store.Verify(s => s.GetByIdAsync(12), Times.Once);
        }

        [Fact]
        public async Task GetVersionsAsync_ShouldDelegateToStore()
        {
            // Arrange
            ResetMocks();

            var list = new[] { new SchemaEntity { Id = 1 }, new SchemaEntity { Id = 2 } };
            _store.Setup(s => s.GetAllForTopicAsync(Topic)).ReturnsAsync(list);

            // Act
            var result = await _service.GetVersionsAsync(Topic);

            // Assert
            result.Should().BeEquivalentTo(list);
        }
    }
}
