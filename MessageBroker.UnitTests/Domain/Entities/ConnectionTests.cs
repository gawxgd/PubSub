using System.Diagnostics;
using FluentAssertions;
using MessageBroker.Domain.Entities;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Domain.Entities;

public class ConnectionTests
{
    [Fact]
    public void Constructor_Should_Set_All_Properties()
    {
        // Arrange
        var logger = Substitute.For<LoggerLib.ILogger>();
        var id = 1L;
        var endpoint = "127.0.0.1:5000";
        var cts = new CancellationTokenSource();
        var task = Task.CompletedTask;

        // Act
        var connection = new Connection(id, endpoint, cts, task, logger);

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
        var logger = Substitute.For<LoggerLib.ILogger>();
        var cts = new CancellationTokenSource();
        var tcs = new TaskCompletionSource();
        var connection = new Connection(1, "test", cts, tcs.Task, logger);

        // Act
        var disconnectTask = connection.DisconnectAsync();
        tcs.SetResult(); // Complete the task
        await disconnectTask;

        // Assert
        cts.Token.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public async Task DisconnectAsync_Should_Wait_For_Task_To_Complete()
    {
        // Arrange
        var logger = Substitute.For<LoggerLib.ILogger>();
        var cts = new CancellationTokenSource();
        var tcs = new TaskCompletionSource();
        var connection = new Connection(1, "test", cts, tcs.Task, logger);
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
        var logger = Substitute.For<LoggerLib.ILogger>();
        var cts = new CancellationTokenSource();
        var tcs = new TaskCompletionSource();
        var connection = new Connection(1, "test", cts, tcs.Task, logger);

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
        var logger = Substitute.For<LoggerLib.ILogger>();
        var cts = new CancellationTokenSource();
        var connection = new Connection(1, "test", cts, Task.CompletedTask, logger);

        // Act
        await connection.DisconnectAsync();
        await connection.DisconnectAsync(); // Call again

        // Assert - should not throw
        cts.Token.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public void Dispose_Should_Dispose_CancellationTokenSource()
    {
        // Arrange
        var logger = Substitute.For<LoggerLib.ILogger>();
        var cts = new CancellationTokenSource();
        var connection = new Connection(1, "test", cts, Task.CompletedTask, logger);

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
        var logger = Substitute.For<LoggerLib.ILogger>();
        var cts = new CancellationTokenSource();
        var connection = new Connection(1, "test", cts, Task.CompletedTask, logger);

        // Act
        connection.Dispose();
        connection.Dispose(); // Call again

        // Assert - should not throw
        var act = () => cts.Token.Register(() => { });
        act.Should().Throw<ObjectDisposedException>();
    }
}