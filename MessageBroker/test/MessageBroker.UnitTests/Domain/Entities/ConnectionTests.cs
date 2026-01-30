using System.Diagnostics;
using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Domain.Entities;

public class ConnectionTests
{
    public ConnectionTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);
    }

    [Fact]
    public void Constructor_Should_Set_All_Properties()
    {
        // Arrange
        var id = 1L;
        var endpoint = "127.0.0.1:5000";
        var cts = new CancellationTokenSource();
        var task = Task.CompletedTask;

        // Act
        var connection = new Connection(id, endpoint, cts, task, ConnectionType.Publisher);

        // Assert
        connection.Id.Should().Be(id);
        connection.ClientEndpoint.Should().Be(endpoint);
        connection.ConnectedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
        connection.CancellationTokenSource.Should().Be(cts);
    }

    [Fact]
    public async Task DisconnectAsync_Should_Cancel_Token()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var tcs = new TaskCompletionSource();
        var connection = new Connection(1, "test", cts, tcs.Task, ConnectionType.Publisher);

        // Act
        var disconnectTask = connection.DisconnectAsync();
        tcs.SetResult();
        await disconnectTask;

        // Assert
        cts.Token.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public async Task DisconnectAsync_Should_Wait_For_Task_To_Complete()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var tcs = new TaskCompletionSource();
        var connection = new Connection(1, "test", cts, tcs.Task, ConnectionType.Publisher);
        var completed = false;

        // Act
        var disconnectTask = Task.Run(async () =>
        {
            await connection.DisconnectAsync();
            completed = true;
        });

        await Task.Delay(100);
        completed.Should().BeFalse("Should wait for task");

        tcs.SetResult();
        await disconnectTask;

        // Assert
        completed.Should().BeTrue();
    }

    [Fact]
    public async Task DisconnectAsync_Should_Timeout_After_5_Seconds()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var tcs = new TaskCompletionSource();
        var connection = new Connection(1, "test", cts, tcs.Task, ConnectionType.Publisher);

        // Act
        var stopwatch = Stopwatch.StartNew();
        await connection.DisconnectAsync();
        stopwatch.Stop();

        // Assert
        stopwatch.Elapsed.Should().BeCloseTo(TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task DisconnectAsync_Should_Be_Idempotent()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var connection = new Connection(1, "test", cts, Task.CompletedTask, ConnectionType.Publisher);

        // Act
        await connection.DisconnectAsync();
        await connection.DisconnectAsync();

        // Assert - should not throw
        cts.Token.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public void Dispose_Should_Dispose_CancellationTokenSource()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var connection = new Connection(1, "test", cts, Task.CompletedTask, ConnectionType.Publisher);

        // Act
        connection.Dispose();

        // Assert
        var act = () => cts.Token.Register(() => { });
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Dispose_Should_Be_Idempotent()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var connection = new Connection(1, "test", cts, Task.CompletedTask, ConnectionType.Publisher);

        // Act
        connection.Dispose();
        connection.Dispose();

        // Assert - should not throw
        var act = () => cts.Token.Register(() => { });
        act.Should().Throw<ObjectDisposedException>();
    }
}
