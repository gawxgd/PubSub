using System.Net.Sockets;
using System.Text;
using FluentAssertions;
using MessageBroker.E2ETests.Infrastructure;
using Publisher.Outbound.Adapter;
using Xunit;

namespace MessageBroker.E2ETests;

public class TcpServerEdgeCasesE2ETests
{
    private const string HostAddress = "127.0.0.1";

    [Fact]
    public async Task Should_Handle_Client_Abrupt_Disconnect()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.UTF8.GetBytes("Hello"));
        await stream.FlushAsync();

        client.Close();

        await Task.Delay(500);

        using var newClient = new TcpClient();
        await newClient.ConnectAsync(HostAddress, port);
        newClient.Connected.Should().BeTrue();

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Empty_Messages()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        var stream = client.GetStream();

        await stream.WriteAsync(new byte[0]);
        await stream.FlushAsync();

        await Task.Delay(500);

        client.Connected.Should().BeTrue();

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Large_Message()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        var stream = client.GetStream();

        var largeData = new byte[1024 * 1024];
        new Random().NextBytes(largeData);

        await stream.WriteAsync(largeData);
        await stream.FlushAsync();

        var buffer = new byte[1024 * 1024];
        var totalReceived = 0;
        var startTime = DateTime.UtcNow;

        while (totalReceived < largeData.Length && (DateTime.UtcNow - startTime).TotalSeconds < 5)
        {
            if (stream.DataAvailable)
            {
                var received = await stream.ReadAsync(buffer.AsMemory(totalReceived));
                if (received == 0)
                {
                    break;
                }

                totalReceived += received;
            }

            await Task.Delay(10);
        }

        totalReceived.Should().Be(largeData.Length, "Server should echo back large message");

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Rapid_Successive_Messages()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        var stream = client.GetStream();
        var messageCount = 100;
        var messages = new List<string>();

        for (var i = 0; i < messageCount; i++)
        {
            var msg = $"Message-{i}";
            messages.Add(msg);
            await stream.WriteAsync(Encoding.UTF8.GetBytes(msg));
        }

        await stream.FlushAsync();

        await Task.Delay(1000);

        client.Connected.Should().BeTrue();

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Client_That_Only_Connects_But_Never_Sends()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var idleClient = new TcpClient();
        await idleClient.ConnectAsync(HostAddress, port);

        await Task.Delay(2000);

        idleClient.Connected.Should().BeTrue();

        using var activeClient = new TcpClient();
        await activeClient.ConnectAsync(HostAddress, port);

        var stream = activeClient.GetStream();
        var testMsg = "Hello";
        await stream.WriteAsync(Encoding.UTF8.GetBytes(testMsg));
        await stream.FlushAsync();

        var buffer = new byte[1024];
        var bytesRead = await stream.ReadAsync(buffer);
        var response = Encoding.UTF8.GetString(buffer, 0, bytesRead);

        response.Should().Be(testMsg);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Maximum_Concurrent_Connections()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        var clients = new List<TcpClient>();
        var connectionCount = 100;

        try
        {

            var connectionTasks = Enumerable.Range(0, connectionCount)
                .Select(async i =>
                {
                    var client = new TcpClient();
                    await client.ConnectAsync(HostAddress, port);
                    clients.Add(client);
                    return client;
                });

            await Task.WhenAll(connectionTasks);

            clients.Count.Should().Be(connectionCount, "All clients should connect successfully");
            clients.Should().OnlyContain(c => c.Connected, "All clients should be connected");
        }
        finally
        {
            foreach (var client in clients)
            {
                client?.Dispose();
            }
        }

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Binary_Data_With_Null_Bytes()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        var stream = client.GetStream();

        var binaryData = new byte[] { 0x00, 0xFF, 0x00, 0x01, 0x00, 0x7F };

        await stream.WriteAsync(binaryData);
        await stream.FlushAsync();

        var buffer = new byte[1024];
        var bytesRead = await stream.ReadAsync(buffer);

        bytesRead.Should().Be(binaryData.Length);
        buffer.Take(bytesRead).Should().Equal(binaryData);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Slow_Reader_Client()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        var stream = client.GetStream();

        var message = "Test message";
        await stream.WriteAsync(Encoding.UTF8.GetBytes(message));
        await stream.FlushAsync();

        await Task.Delay(2000);

        var buffer = new byte[1024];
        var bytesRead = await stream.ReadAsync(buffer);
        var response = Encoding.UTF8.GetString(buffer, 0, bytesRead);

        response.Should().Be(message);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Connection_During_Shutdown()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        var shutdownTask = host.StopAsync();

        var exception = await Record.ExceptionAsync(async () =>
        {
            using var client = new TcpClient();
            await client.ConnectAsync(HostAddress, port);
        });

        if (exception != null)
        {
            exception.Should().BeOfType<SocketException>("Connection during shutdown should fail with SocketException");
        }

        await shutdownTask;
    }

    [Fact]
    public async Task Should_Handle_Half_Closed_Connection()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();
        await client.ConnectAsync(HostAddress, port);

        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.UTF8.GetBytes("Hello"));
        await stream.FlushAsync();

        client.Client.Shutdown(SocketShutdown.Send);

        await Task.Delay(500);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Reject_Connection_On_Wrong_Port()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();

        var exception = await Record.ExceptionAsync(async () =>
        {
            await client.ConnectAsync(HostAddress, port + 1);
        });

        exception.Should().BeOfType<SocketException>();

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Client_Socket_Options_Manipulation()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        using var client = new TcpClient();

        client.SendTimeout = 100;
        client.ReceiveTimeout = 100;

        await client.ConnectAsync(HostAddress, port);

        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.UTF8.GetBytes("Test"));
        await stream.FlushAsync();

        await Task.Delay(500);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Publisher_Abrupt_Disconnect()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        var publisher = new TcpPublisher(HostAddress, port, 1000, 3, 5);
        await publisher.CreateConnection();

        await publisher.PublishAsync(Encoding.UTF8.GetBytes("Test message 1"));
        await publisher.PublishAsync(Encoding.UTF8.GetBytes("Test message 2"));
        await Task.Delay(100);

        await publisher.DisposeAsync();

        await Task.Delay(500);

        await using var newPublisher = new TcpPublisher(HostAddress, port, 1000, 3, 5);
        await newPublisher.CreateConnection();
        await newPublisher.PublishAsync(Encoding.UTF8.GetBytes("New publisher message"));
        await Task.Delay(100);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Publisher_Large_Messages()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        await using var publisher = new TcpPublisher(HostAddress, port, 10000, 3, 5);
        await publisher.CreateConnection();

        var largeData = new byte[1024 * 1024];
        new Random().NextBytes(largeData);

        await publisher.PublishAsync(largeData);
        await Task.Delay(2000);

        await publisher.PublishAsync(Encoding.UTF8.GetBytes("Small message after large"));
        await Task.Delay(100);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Publisher_Rapid_Successive_Messages()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        await using var publisher = new TcpPublisher(HostAddress, port, 10000, 3, 5);
        await publisher.CreateConnection();

        var messageCount = 200;

        for (var i = 0; i < messageCount; i++)
        {
            var msg = Encoding.UTF8.GetBytes($"Rapid message {i}");
            await publisher.PublishAsync(msg);
        }

        await Task.Delay(2000);

        await publisher.PublishAsync(Encoding.UTF8.GetBytes("Final message"));
        await Task.Delay(100);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Publisher_Connection_During_Shutdown()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        var shutdownTask = host.StopAsync();

        var exception = await Record.ExceptionAsync(async () =>
        {
            await using var publisher = new TcpPublisher(HostAddress, port, 1000, 3, 5);
            await publisher.CreateConnection();
        });

        if (exception != null)
        {
            exception.Should()
                .BeOfType<SocketException>("Publisher connection during shutdown should fail with SocketException");
        }

        await shutdownTask;
    }

    [Fact]
    public async Task Should_Handle_Publisher_With_Binary_Data()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        await using var publisher = new TcpPublisher(HostAddress, port, 1000, 3, 5);
        await publisher.CreateConnection();

        var binaryData = new byte[] { 0x00, 0xFF, 0x00, 0x01, 0x00, 0x7F };

        await publisher.PublishAsync(binaryData);
        await Task.Delay(500);

        await publisher.PublishAsync(Encoding.UTF8.GetBytes("Text message after binary"));
        await Task.Delay(100);

        await host.StopAsync();
    }

    [Fact]
    public async Task Should_Handle_Publisher_Reconnection_After_Server_Restart()
    {
        var port = PortManager.GetNextPort();

        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        await using var publisher = new TcpPublisher(HostAddress, port, 1000, 3, 5);
        await publisher.CreateConnection();
        await publisher.PublishAsync(Encoding.UTF8.GetBytes("Before restart"));
        await Task.Delay(100);

        await host.StopAsync();
        await Task.Delay(1000);

        using var newHost = TestHostHelper.CreateTestHost(port);
        await newHost.StartAsync();
        await Task.Delay(500);

        await Task.Delay(2000);
        await publisher.PublishAsync(Encoding.UTF8.GetBytes("After restart"));
        await Task.Delay(100);

        await newHost.StopAsync();
    }
}
