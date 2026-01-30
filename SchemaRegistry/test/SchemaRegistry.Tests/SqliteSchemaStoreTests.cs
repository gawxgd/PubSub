using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Infrastructure.Adapter;
using Xunit;

namespace SchemaRegistry.Tests;

public class SqliteSchemaStoreTests : IDisposable
{
    private readonly string _dbPath;
    private readonly SqliteSchemaStore _store;

    public SqliteSchemaStoreTests()
    {
        _dbPath = Path.Combine(
            Path.GetTempPath(),
            $"schema-registry-test-{Guid.NewGuid():N}.db");

        var connectionString = $"Data Source={_dbPath}";
        _store = new SqliteSchemaStore(connectionString);
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
        {
            File.Delete(_dbPath);
        }
    }

    [Fact]
    public async Task SaveAsync_ShouldPersistAndRetrieveById()
    {
        var entity = new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{ \"type\": \"record\", \"name\": \"User\" }",
            Checksum = "checksum-1"
        };

        var saved = await _store.SaveAsync(entity);

        saved.Id.Should().BeGreaterThan(0);
        saved.Version.Should().Be(1);

        var loaded = await _store.GetByIdAsync(saved.Id);

        loaded.Should().NotBeNull();
        loaded!.Topic.Should().Be("users");
        loaded.SchemaJson.Should().Be(entity.SchemaJson);
        loaded.Version.Should().Be(1);
    }

    [Fact]
    public async Task SaveAsync_ShouldIncrementVersionPerTopic()
    {
        await _store.SaveAsync(new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{}",
            Checksum = "c1"
        });

        var second = await _store.SaveAsync(new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{}",
            Checksum = "c2"
        });

        second.Version.Should().Be(2);
    }

    [Fact]
    public async Task GetLatestForTopicAsync_ShouldReturnHighestVersion()
    {
        await _store.SaveAsync(new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{}",
            Checksum = "c1"
        });

        var latest = await _store.SaveAsync(new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{}",
            Checksum = "c2"
        });

        var result = await _store.GetLatestForTopicAsync("users");

        result.Should().NotBeNull();
        result!.Id.Should().Be(latest.Id);
        result.Version.Should().Be(2);
    }

    [Fact]
    public async Task GetAllForTopicAsync_ShouldReturnAllVersionsOrderedDesc()
    {
        await _store.SaveAsync(new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{}",
            Checksum = "c1"
        });

        await _store.SaveAsync(new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{}",
            Checksum = "c2"
        });

        var all = (await _store.GetAllForTopicAsync("users")).ToList();

        all.Should().HaveCount(2);
        all[0].Version.Should().Be(2);
        all[1].Version.Should().Be(1);
    }

    [Fact]
    public async Task GetByChecksumAsync_ShouldReturnMatchingSchema()
    {
        var saved = await _store.SaveAsync(new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{}",
            Checksum = "unique-checksum"
        });

        var result = await _store.GetByChecksumAsync("unique-checksum");

        result.Should().NotBeNull();
        result!.Id.Should().Be(saved.Id);
    }

    [Fact]
    public async Task Data_ShouldPersist_AfterStoreRecreation()
    {
        var entity = new SchemaEntity
        {
            Topic = "users",
            SchemaJson = "{}",
            Checksum = "persisted"
        };

        var saved = await _store.SaveAsync(entity);

        var newStore = new SqliteSchemaStore($"Data Source={_dbPath}");

        var loaded = await newStore.GetByIdAsync(saved.Id);

        loaded.Should().NotBeNull();
        loaded!.Checksum.Should().Be("persisted");
    }
}
