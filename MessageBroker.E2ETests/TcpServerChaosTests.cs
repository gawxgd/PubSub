// MessageBroker.E2ETests/TcpServerChaosTests.cs
using System.Net.Sockets;
using FluentAssertions;
using MessageBroker.TcpServer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MessageBroker.E2ETests;

public class TcpServerChaosTests
{
    private const int Port = 9096;
    private const string HostAddress = "127.0.0.1";

    [Xunit.Fact]
    public async Task Chaos_Random_Client_Behavior()
    {
        using var host = CreateTestHost();
        await host.StartAsync();
        await Task.Delay(500);

        var random = new Random();
        var clients = new List<TcpClient>();
        var exceptions = new List<Exception>();

        try
        {
            // Create chaos with random client behaviors
            var chaosTasks = Enumerable.Range(0, 20).Select(async i =>
            {
                try
                {
                    var client = new TcpClient();
                    await client.ConnectAsync(HostAddress, Port);
                    clients.Add(client);

                    var behavior = random.Next(5);
                    
                    switch (behavior)
                    {
                        case 0: // Send and disconnect immediately
                            await client.GetStream().WriteAsync(new byte[] { 1, 2, 3 });
                            client.Close();
                            break;
                            
                        case 1: // Send random data
                            var data = new byte[random.Next(1, 1000)];
                            random.NextBytes(data);
                            await client.GetStream().WriteAsync(data);
                            break;
                            
                        case 2: // Just connect and wait
                            await Task.Delay(random.Next(100, 1000));
                            break;
                            
                        case 3: // Rapid fire messages
                            for (int j = 0; j < random.Next(5, 20); j++)
                            {
                                await client.GetStream().WriteAsync(new byte[] { (byte)j });
                            }
                            break;
                            
                        case 4: // Half close
                            await client.GetStream().WriteAsync(new byte[] { 99 });
                            client.Client.Shutdown(SocketShutdown.Send);
                            break;
                    }

                    await Task.Delay(random.Next(0, 500));
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });

            await Task.WhenAll(chaosTasks);

            // Server should survive chaos
            using var testClient = new TcpClient();
            await testClient.ConnectAsync(HostAddress, Port);
            testClient.Connected.Should().BeTrue("Server should survive chaos and accept new connections");
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

    private static IHost CreateTestHost()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Port"] = Port.ToString(),
                ["Address"] = HostAddress,
                ["MaxRequestSizeInByte"] = "512",
                ["InlineCompletions"] = "false",
                ["SocketPolling"] = "false",
                ["Backlog"] = "100"
            })
            .Build();
        
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<CreateSocketUseCase>();
                services.AddHostedService<TcpServerService>();
                services.Configure<TcpServerOptions>(configuration);
            })
            .Build();
    }
}