using Microsoft.Extensions.Configuration;
using Publisher.Outbound.Adapter;
using PubSubDemo.Configuration;
using PubSubDemo.Services;

Console.WriteLine("╔════════════════════════════════════════════╗");
Console.WriteLine("║   PubSub Demo - Message Publisher         ║");
Console.WriteLine("║   Demonstrates TcpPublisher Usage          ║");
Console.WriteLine("╚════════════════════════════════════════════╝");
Console.WriteLine();

// Load configuration
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", false, true)
    .AddEnvironmentVariables("PUBSUB_")
    .AddCommandLine(args)
    .Build();

var brokerOptions = configuration.GetSection("Broker").Get<BrokerOptions>() ?? new BrokerOptions();
var demoOptions = configuration.GetSection("Demo").Get<DemoOptions>() ?? new DemoOptions();

Console.WriteLine("📡 Broker Configuration:");
Console.WriteLine($"   Host: {brokerOptions.Host}");
Console.WriteLine($"   Port: {brokerOptions.Port}");
Console.WriteLine($"   Queue Size: {brokerOptions.MaxQueueSize}");
Console.WriteLine();

Console.WriteLine("⚙️  Demo Configuration:");
Console.WriteLine($"   Message Interval: {demoOptions.MessageInterval}ms");
Console.WriteLine($"   Message Prefix: {demoOptions.MessagePrefix}");
Console.WriteLine($"   Batch Size: {demoOptions.BatchSize}");
Console.WriteLine();

// Setup cancellation
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\n\n🛑 Shutdown signal received...");
};

try
{
    // Create publisher
    await using var publisher = new TcpPublisher(
        brokerOptions.Host,
        brokerOptions.Port,
        brokerOptions.MaxQueueSize, 5, 5);

    // Create publishing service
    await using var publisherService = new MessagePublisherService(publisher, demoOptions);

    // Start the service
    await publisherService.StartAsync(cts.Token);

    Console.WriteLine("🚀 Publishing messages... Press Ctrl+C to stop.\n");

    // Wait for cancellation
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("✅ Graceful shutdown complete.");
}
catch (Exception ex)
{
    Console.WriteLine($"❌ Fatal error: {ex.Message}");
    Console.WriteLine($"   Type: {ex.GetType().Name}");

    if (ex.InnerException != null)
    {
        Console.WriteLine($"   Inner: {ex.InnerException.Message}");
    }

    return 1;
}

Console.WriteLine("\n👋 PubSub Demo finished.");
return 0;

