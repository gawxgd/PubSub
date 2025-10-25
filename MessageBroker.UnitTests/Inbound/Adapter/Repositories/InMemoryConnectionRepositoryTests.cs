using System.Collections.Concurrent;
using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Inbound.Adapter;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.Adapter.Repositories;

public class InMemoryConnectionRepositoryTests
{
    public InMemoryConnectionRepositoryTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);
    }

    [Fact]
    public void GenerateConnectionId_Should_Return_Sequential_Ids()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();

        // Act
        var id1 = repository.GenerateConnectionId();
        var id2 = repository.GenerateConnectionId();
        var id3 = repository.GenerateConnectionId();

        // Assert
        id1.Should().Be(1);
        id2.Should().Be(2);
        id3.Should().Be(3);
    }

    [Fact]
    public void GenerateConnectionId_Should_Be_Thread_Safe()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();
        var ids = new ConcurrentBag<long>();

        // Act
        Parallel.For(0, 1000, _ =>
        {
            var id = repository.GenerateConnectionId();
            ids.Add(id);
        });

        // Assert
        ids.Should().HaveCount(1000);
        ids.Distinct().Should().HaveCount(1000, "All IDs should be unique");
    }

    [Fact]
    public void Add_Should_Store_Connection()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();
        var connection = CreateTestConnection(1);

        // Act
        repository.Add(connection);

        // Assert
        var retrieved = repository.Get(1);
        retrieved.Should().BeSameAs(connection);
    }

    [Fact]
    public void Add_Should_Throw_When_Duplicate_Id()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();
        var connection1 = CreateTestConnection(1);
        var connection2 = CreateTestConnection(1);
        repository.Add(connection1);

        // Act
        var act = () => repository.Add(connection2);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Connection with id 1 already exists.");
    }

    [Fact]
    public void Remove_Should_Remove_Connection()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();
        var cts = new CancellationTokenSource();
        var connection = new Connection(1, "test", cts, Task.CompletedTask);
        repository.Add(connection);

        // Act
        var result = repository.Remove(1);

        // Assert
        result.Should().BeTrue();
        repository.Get(1).Should().BeNull();
    }

    [Fact]
    public void Remove_Should_Return_False_When_Not_Found()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();

        // Act
        var result = repository.Remove(999);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void Get_Should_Return_Null_When_Not_Found()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();

        // Act
        var result = repository.Get(999);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void GetAll_Should_Return_All_Connections()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();
        var conn1 = CreateTestConnection(1);
        var conn2 = CreateTestConnection(2);
        var conn3 = CreateTestConnection(3);
        repository.Add(conn1);
        repository.Add(conn2);
        repository.Add(conn3);

        // Act
        var all = repository.GetAll();

        // Assert
        all.Should().HaveCount(3);
        all.Should().Contain(conn1);
        all.Should().Contain(conn2);
        all.Should().Contain(conn3);
    }

    [Fact]
    public void RemoveAll_Should_Clear_All_Connections()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();
        var conn1 = CreateTestConnection(1);
        var conn2 = CreateTestConnection(2);
        var conn3 = CreateTestConnection(3);

        repository.Add(conn1);
        repository.Add(conn2);
        repository.Add(conn3);

        // Act
        repository.RemoveAll();

        // Assert
        repository.GetAll().Should().BeEmpty("All connections should be cleared");
    }

    [Fact]
    public void RemoveAll_Should_Handle_Empty_Repository()
    {
        // Arrange
        var repository = new InMemoryConnectionRepository();

        // Act
        var act = () => repository.RemoveAll();

        // Assert
        act.Should().NotThrow();
    }

    private static Connection CreateTestConnection(long id)
    {
        return new Connection(id, $"test-{id}", new CancellationTokenSource(), Task.CompletedTask);
    }
}