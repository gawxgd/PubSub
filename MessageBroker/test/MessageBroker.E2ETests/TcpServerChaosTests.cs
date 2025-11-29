// MessageBroker.E2ETests/TcpServerChaosTests.cs

using System.Net.Sockets;
using System.Text;
using FluentAssertions;
using MessageBroker.E2ETests.Infrastructure;
using Publisher.Outbound.Adapter;
using Xunit;

namespace MessageBroker.E2ETests;

public class TcpServerChaosTests
{
    [Fact]
    public async Task Chaos_Random_Client_Behavior()
    {
        var port = PortManager.GetNextPort();
        const string hostAddress = "127.0.0.1";
        using var host = TestHostHelper.CreateTestHost(port);
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
                    await client.ConnectAsync(hostAddress, port);
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
                            for (var j = 0; j < random.Next(5, 20); j++)
                            {
                                await client.GetStream().WriteAsync(new[] { (byte)j });
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
            await testClient.ConnectAsync(hostAddress, port);
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

    [Fact]
    public async Task Chaos_Publisher_Resilience_Test()
    {
        var port = PortManager.GetNextPort();
        const string hostAddress = "127.0.0.1";
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        var random = new Random();
        var publishers = new List<TcpPublisher>();
        var exceptions = new List<Exception>();

        try
        {
            // Create chaos with random publisher behaviors
            var chaosTasks = Enumerable.Range(0, 10).Select(async i =>
            {
                try
                {
                    var publisher = new TcpPublisher(hostAddress, port, "topic", 1000, 3, 5);
                    await publisher.CreateConnection();
                    publishers.Add(publisher);

                    var behavior = random.Next(6);

                    switch (behavior)
                    {
                        case 0: // Send and disconnect immediately
                            await publisher.PublishAsync(Encoding.UTF8.GetBytes($"Quick message {i}"));
                            await publisher.DisposeAsync();
                            break;

                        case 1: // Send random data
                            var data = new byte[random.Next(1, 1000)];
                            random.NextBytes(data);
                            await publisher.PublishAsync(data);
                            break;

                        case 2: // Send many messages rapidly
                            for (var j = 0; j < random.Next(5, 20); j++)
                            {
                                var message = Encoding.UTF8.GetBytes($"Rapid message {i}-{j}");
                                await publisher.PublishAsync(message);
                            }

                            break;

                        case 3: // Send large messages
                            var largeData = new byte[random.Next(1000, 10000)];
                            random.NextBytes(largeData);
                            await publisher.PublishAsync(largeData);
                            break;

                        case 4: // Send binary data
                            var binaryData = new byte[] { 0x00, 0xFF, 0x00, 0x01, 0x00, 0x7F };
                            await publisher.PublishAsync(binaryData);
                            break;

                        case 5: // Just connect and wait
                            await Task.Delay(random.Next(100, 1000));
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
            await using var testPublisher = new TcpPublisher(hostAddress, port, "test-topic", 1000, 3, 5);
            await testPublisher.CreateConnection();
            await testPublisher.PublishAsync(Encoding.UTF8.GetBytes("Test after chaos"));
            await Task.Delay(100);
        }
        finally
        {
            foreach (var publisher in publishers)
            {
                try
                {
                    await publisher.DisposeAsync();
                }
                catch
                {
                    // Ignore disposal errors in chaos test
                }
            }
        }

        await host.StopAsync();
    }

    [Fact]
    public async Task Chaos_Publisher_Connection_Storm()
    {
        var port = PortManager.GetNextPort();
        const string hostAddress = "127.0.0.1";
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        var publishers = new List<TcpPublisher>();
        var tasks = new List<Task>();

        try
        {
            // Create connection storm - many publishers connecting/disconnecting rapidly
            for (var i = 0; i < 50; i++)
            {
                var publisherId = i;
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var publisher = new TcpPublisher(hostAddress, port, "storm-topic", 1000, 3, 5);
                        await publisher.CreateConnection();
                        publishers.Add(publisher);

                        // Send some messages
                        for (var j = 0; j < 5; j++)
                        {
                            var message = Encoding.UTF8.GetBytes($"Storm message {publisherId}-{j}");
                            await publisher.PublishAsync(message);
                        }

                        // Randomly disconnect some publishers
                        if (Random.Shared.Next(2) == 0)
                        {
                            await publisher.DisposeAsync();
                        }
                    }
                    catch
                    {
                        // Ignore connection errors in storm test
                    }
                }));
            }

            await Task.WhenAll(tasks);
            await Task.Delay(2000); // Allow processing time

            // Server should still be functional
            await using var testPublisher = new TcpPublisher(hostAddress, port, "storm-topic", 1000, 3, 5);
            await testPublisher.CreateConnection();
            await testPublisher.PublishAsync(Encoding.UTF8.GetBytes("Test after storm"));
            await Task.Delay(100);
        }
        finally
        {
            foreach (var publisher in publishers)
            {
                try
                {
                    await publisher.DisposeAsync();
                }
                catch
                {
                    // Ignore disposal errors
                }
            }
        }

        await host.StopAsync();
    }

    [Fact]
    public async Task Chaos_Publisher_Message_Flood()
    {
        var port = PortManager.GetNextPort();
        const string hostAddress = "127.0.0.1";
        using var host = TestHostHelper.CreateTestHost(port);
        await host.StartAsync();
        await Task.Delay(500);

        var publishers = new List<TcpPublisher>();
        var tasks = new List<Task>();

        try
        {
            // Create multiple publishers flooding the server with messages
            for (var i = 0; i < 10; i++)
            {
                var publisher = new TcpPublisher(hostAddress, port, "flood-topic", 10000, 3, 5);
                await publisher.CreateConnection();
                publishers.Add(publisher);

                var publisherId = i;
                tasks.Add(Task.Run(async () =>
                {
                    // Each publisher sends many messages
                    for (var j = 0; j < 100; j++)
                    {
                        var message = Encoding.UTF8.GetBytes($"Flood message {publisherId}-{j}");
                        await publisher.PublishAsync(message);

                        // Small random delay to create realistic chaos
                        if (Random.Shared.Next(10) == 0)
                        {
                            await Task.Delay(Random.Shared.Next(1, 10));
                        }
                    }
                }));
            }

            await Task.WhenAll(tasks);
            await Task.Delay(3000); // Allow processing time

            // Server should still be functional
            await using var testPublisher = new TcpPublisher(hostAddress, port, "flood-topic", 1000, 3, 5);
            await testPublisher.CreateConnection();
            await testPublisher.PublishAsync(Encoding.UTF8.GetBytes("Test after flood"));
            await Task.Delay(100);
        }
        finally
        {
            foreach (var publisher in publishers)
            {
                try
                {
                    await publisher.DisposeAsync();
                }
                catch
                {
                    // Ignore disposal errors
                }
            }
        }

        await host.StopAsync();
    }
}