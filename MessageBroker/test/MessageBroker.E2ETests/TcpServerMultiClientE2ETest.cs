using System.Net.Sockets;
using System.Text;
using FluentAssertions;
using MessageBroker.E2ETests.Infrastructure;
using Xunit;

namespace MessageBroker.E2ETests;

public class TcpServerMultiClientE2ETests
{
    private const string HostAddress = "127.0.0.1";

    [Fact]
    public async Task Server_Should_Handle_Multiple_Clients_And_Echo_Back()
    {
        var port = PortManager.GetNextPort();
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();

        // Allow the server time to start listening
        await Task.Delay(300);

        const int clientCount = 3;
        var clients = new TcpClient[clientCount];
        var sendTasks = new List<Task<string>>();

        // Connect all clients
        for (var i = 0; i < clientCount; i++)
        {
            clients[i] = new TcpClient();
            await clients[i].ConnectAsync(HostAddress, port);

            var clientId = i;
            sendTasks.Add(Task.Run(() => SendAndReceiveAsync(clients[clientId], $"Hello from client {clientId}")));
        }

        // Wait for all clients to complete
        var results = await Task.WhenAll(sendTasks);

        // Verify all responses are correct
        for (var i = 0; i < clientCount; i++) results[i].Should().Be($"Hello from client {i}");

        // Cleanup
        foreach (var client in clients)
            client.Dispose();

        await host.StopAsync();
    }

    private static async Task<string> SendAndReceiveAsync(TcpClient client, string message)
    {
        var stream = client.GetStream();
        var sendBuffer = Encoding.UTF8.GetBytes(message);

        await stream.WriteAsync(sendBuffer);
        await stream.FlushAsync();

        var receiveBuffer = new byte[1024];
        var bytesRead = await stream.ReadAsync(receiveBuffer);

        return Encoding.UTF8.GetString(receiveBuffer, 0, bytesRead);
    }
}