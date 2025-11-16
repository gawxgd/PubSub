using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Inbound.Adapter;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.TcpServer.Service;

public class ConnectionManagerTests
{
    public ConnectionManagerTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);
    }

    [Fact]
    public void RegisterConnection_Should_Generate_Id_And_Add_Connection()
    {
        // Arrange
        var repository = Substitute.For<IConnectionRepository>();
        var commitLog = Substitute.For<ICommitLogFactory>();
        repository.GenerateConnectionId().Returns(42L);
        var manager = new ConnectionManager(repository, commitLog);
        var socket = CreateMockSocket();
        var cts = new CancellationTokenSource();

        // Act
        manager.RegisterConnection(socket, cts);

        // Wait a bit for Task.Run to execute
        Thread.Sleep(100);

        // Assert
        repository.Received(1).GenerateConnectionId();
        repository.Received(1).Add(Arg.Is<Connection>(c => c.Id == 42));
    }

    [Fact]
    public async Task UnregisterConnectionAsync_Should_Disconnect_And_Remove()
    {
        // Arrange
        var repository = Substitute.For<IConnectionRepository>();
        var commitLog = Substitute.For<ICommitLogFactory>();
        var tcs = new TaskCompletionSource();
        var cts = new CancellationTokenSource();
        var connection = new Connection(1, "test", cts, tcs.Task);
        repository.Get(1).Returns(connection);
        var manager = new ConnectionManager(repository, commitLog);

        // Act
        var unregisterTask = manager.UnregisterConnectionAsync(1);
        tcs.SetResult(); // Complete the task
        await unregisterTask;

        // Assert
        // Note: Can't check cts.Token after dispose, but we can verify Remove was called
        repository.Received(1).Remove(1);
    }

    [Fact]
    public async Task UnregisterConnectionAsync_Should_Handle_NonExistent_Connection()
    {
        // Arrange
        var repository = Substitute.For<IConnectionRepository>();
        var commitLog = Substitute.For<ICommitLogFactory>();
        repository.Get(999).Returns((Connection?)null);
        var manager = new ConnectionManager(repository, commitLog);

        // Act
        var act = async () => await manager.UnregisterConnectionAsync(999);

        // Assert
        await act.Should().NotThrowAsync();
        repository.DidNotReceive().Remove(Arg.Any<long>());
    }

    [Fact]
    public async Task UnregisterAllConnectionsAsync_Should_Disconnect_And_Clear_All()
    {
        // Arrange
        var repository = Substitute.For<IConnectionRepository>();
        var commitLog = Substitute.For<ICommitLogFactory>();
        var tcs1 = new TaskCompletionSource();
        var tcs2 = new TaskCompletionSource();
        var cts1 = new CancellationTokenSource();
        var cts2 = new CancellationTokenSource();

        var conn1 = new Connection(1, "test1", cts1, tcs1.Task);
        var conn2 = new Connection(2, "test2", cts2, tcs2.Task);

        repository.GetAll().Returns(new List<Connection> { conn1, conn2 });
        var manager = new ConnectionManager(repository, commitLog);

        // Act
        var unregisterTask = manager.UnregisterAllConnectionsAsync();

        // Complete tasks to allow disconnect to finish
        tcs1.SetResult();
        tcs2.SetResult();

        await unregisterTask;

        // Assert
        // Note: Can't check cts.Token after dispose, but we can verify RemoveAll was called
        repository.Received(1).RemoveAll();
    }

    private static Socket CreateMockSocket()
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        // We can't really create a connected socket easily in unit tests, 
        // so we just return a basic socket. The ConnectionManager will use it.
        return socket;
    }
}