using Microsoft.Extensions.Configuration;
using Publisher.Outbound.Adapter;
using PubSubDemo.Configuration;
using PubSubDemo.Services;
using Subscriber.Configuration;
using Subscriber.Configuration.Options;
using LoggerLib.Outbound.Adapter;
using ILogger = LoggerLib.Domain.Port.ILogger;

// Initialize logger
var logger = new LoggerLib.Outbound.Adapter.ConsoleLogger();
AutoLoggerFactory.Initialize(logger);

Console.WriteLine("╔════════════════════════════════════════════╗");
Console.WriteLine("║   PubSub Full Demo - Publisher & Subscriber ║");
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

    // Create subscriber
    var subscriberOptions = new SubscriberOptions
    {
        MessageBrokerConnectionUri = new Uri($"messageBroker://{brokerOptions.Host}:{brokerOptions.Port}"),
        Topic = "default",
        MinMessageLength = 1,
        MaxMessageLength = 1000,
        PollInterval = TimeSpan.FromMilliseconds(100),
        MaxRetryAttempts = 3
        // MaxQueueSize is read-only, uses default value 65536
    };

    var subscriberFactory = new SubscriberFactory();
    var subscriber = subscriberFactory.CreateSubscriber(subscriberOptions, async (message) =>
    {
        Console.WriteLine($"📨 Subscriber otrzymał: {message}");
        await Task.CompletedTask;
    });

    // Start everything
    Console.WriteLine("🚀 Uruchamianie Publisher i Subscriber...\n");
    
    // MessagePublisherService.StartAsync() already calls CreateConnection() internally
    await publisherService.StartAsync(cts.Token);
    await subscriber.StartConnectionAsync();
    await subscriber.StartMessageProcessingAsync();

    Console.WriteLine("✅ Wszystko działa! Sprawdź frontend na http://localhost:3000");
    Console.WriteLine("   Naciśnij Ctrl+C aby zatrzymać.\n");

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

