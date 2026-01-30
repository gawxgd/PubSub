using System.Net.Sockets;
using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Port;
using MessageBroker.E2ETests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MessageBroker.E2ETests;

public class TcpServerConnectionTrackingE2ETests
{
    private const string HostAddress = "127.0.0.1";

    [Fact]
    public async Task Should_Track_Connection_When_Client_Connects()
    {
        // Arrange
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        var repository = host.Services.GetRequiredService<IConnectionRepository>();
        await host.StartAsync();
        await Task.Delay(300);

        // Act
        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        await WaitUntilAsync(() => repository.GetAll().Count >= 1, TimeSpan.FromSeconds(3));

        // Assert
        repository.GetAll().Should().HaveCount(1);

        var connection = repository.GetAll().First();
        connection.ClientEndpoint.Should().Contain("127.0.0.1");

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Remove_Connection_When_Client_Disconnects()
    {
        // Arrange
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        var repository = host.Services.GetRequiredService<IConnectionRepository>();
        await host.StartAsync();
        await Task.Delay(300);

        // Act - Connect and disconnect
        using (var client = new TcpClient())
        {
            await client.ConnectAsync(HostAddress, port);
            await WaitUntilAsync(() => repository.GetAll().Count >= 1, TimeSpan.FromSeconds(3));
            repository.GetAll().Should().HaveCount(1);
        }

        await WaitUntilAsync(() => repository.GetAll().Count == 0, TimeSpan.FromSeconds(5));

        // Assert
        repository.GetAll().Should().BeEmpty("Connection should be removed after disconnect");

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Track_Multiple_Concurrent_Connections()
    {
        // Arrange
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        var repository = host.Services.GetRequiredService<IConnectionRepository>();
        await host.StartAsync();
        await Task.Delay(300);

        // Act
        var clients = new List<TcpClient>();
        for (var i = 0; i < 5; i++)
        {
            var client = new TcpClient();
            await client.ConnectAsync(HostAddress, port);
            clients.Add(client);
        }

        await WaitUntilAsync(() => repository.GetAll().Count >= 5, TimeSpan.FromSeconds(3));

        // Assert
        repository.GetAll().Should().HaveCount(5);

        foreach (var client in clients)
        {
            client.Dispose();
        }

        await WaitUntilAsync(() => repository.GetAll().Count == 0, TimeSpan.FromSeconds(5));

        repository.GetAll().Should().BeEmpty();

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Assign_Unique_Ids_To_Connections()
    {
        // Arrange
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        var repository = host.Services.GetRequiredService<IConnectionRepository>();
        await host.StartAsync();
        await Task.Delay(300);

        // Act
        var clients = new List<TcpClient>();
        for (var i = 0; i < 3; i++)
        {
            var client = new TcpClient();
            await client.ConnectAsync(HostAddress, port);
            clients.Add(client);
        }

        await WaitUntilAsync(() => repository.GetAll().Count >= 3, TimeSpan.FromSeconds(3));

        // Assert
        var connections = repository.GetAll();
        var ids = connections.Select(c => c.Id).ToList();
        ids.Should().OnlyHaveUniqueItems();
        ids.Should().BeInAscendingOrder();

        foreach (var client in clients)
        {
            client.Dispose();
        }

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Disconnect_All_Connections_On_Shutdown()
    {
        // Arrange
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        var repository = host.Services.GetRequiredService<IConnectionRepository>();
        await host.StartAsync();
        await Task.Delay(300);

        var clients = new List<TcpClient>();
        for (var i = 0; i < 3; i++)
        {
            var client = new TcpClient();
            await client.ConnectAsync(HostAddress, port);
            clients.Add(client);
        }

        await WaitUntilAsync(() => repository.GetAll().Count >= 3, TimeSpan.FromSeconds(3));
        repository.GetAll().Should().HaveCount(3);

        // Act - Shutdown server
        await host.StopAsync();

        // Assert
        repository.GetAll().Should().BeEmpty("All connections should be disconnected on shutdown");

        foreach (var client in clients)
        {
            client.Dispose();
        }
    }

    [Fact]
    public async Task Should_Track_Connection_Lifecycle_With_Messages()
    {
        // Arrange
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        var repository = host.Services.GetRequiredService<IConnectionRepository>();
        await host.StartAsync();
        await Task.Delay(300);

        // Act
        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        await WaitUntilAsync(() => repository.GetAll().Count >= 1, TimeSpan.FromSeconds(3));

        var stream = client.GetStream();
        var message = "Hello, Server!";
        await stream.WriteAsync(Encoding.UTF8.GetBytes(message));
        await stream.FlushAsync();

        var buffer = new byte[1024];
        var bytesRead = await stream.ReadAsync(buffer);
        var response = Encoding.UTF8.GetString(buffer, 0, bytesRead);

        // Assert
        response.Should().Be(message);

        var connections = repository.GetAll();
        connections.Should().HaveCount(1);
        connections.First().ClientEndpoint.Should().Contain("127.0.0.1");

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Abrupt_Disconnect_And_Update_Tracking()
    {
        // Arrange
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        var repository = host.Services.GetRequiredService<IConnectionRepository>();
        await host.StartAsync();
        await Task.Delay(300);

        // Act
        using (var client = new TcpClient())
        {
            await client.ConnectAsync(HostAddress, port);

            await WaitUntilAsync(() => repository.GetAll().Count >= 1, TimeSpan.FromSeconds(3));
            repository.GetAll().Should().HaveCount(1);

            client.Client.Close();
        }

        await WaitUntilAsync(() => repository.GetAll().Count == 0, TimeSpan.FromSeconds(5));

        // Assert
        repository.GetAll().Should().BeEmpty("Connection should be cleaned up after abrupt disconnect");

        await host.StopAsync();
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var endTime = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < endTime)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(50);
        }

        if (!condition())
        {
            throw new TimeoutException($"Condition was not met within {timeout.TotalSeconds} seconds");
        }
    }
}
