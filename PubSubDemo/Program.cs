using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Http;
using Publisher.Configuration;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using PubSubDemo.Configuration;
using PubSubDemo.Infrastructure;
using PubSubDemo.Services;
using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Domain.Port.SchemaRegistryClient;
using Shared.Outbound.SchemaRegistryClient;
using Subscriber.Configuration;
using Subscriber.Configuration.Options;
using LoggerLib.Outbound.Adapter;

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
var schemaRegistryOptions = configuration.GetSection("SchemaRegistry").Get<SchemaRegistryClientOptions>() 
    ?? new SchemaRegistryClientOptions(
        new Uri("http://localhost:5002"),
        TimeSpan.FromSeconds(10));

Console.WriteLine("📡 Broker Configuration:");
Console.WriteLine($"   Host: {brokerOptions.Host}");
Console.WriteLine($"   Port: {brokerOptions.Port}");
Console.WriteLine($"   Queue Size: {brokerOptions.MaxQueueSize}");
Console.WriteLine();

Console.WriteLine("📋 Schema Registry Configuration:");
Console.WriteLine($"   Base Address: {schemaRegistryOptions.BaseAddress}");
Console.WriteLine($"   Timeout: {schemaRegistryOptions.Timeout}");
Console.WriteLine();

Console.WriteLine("⚙️  Demo Configuration:");
Console.WriteLine($"   Message Interval: {demoOptions.MessageInterval}ms");
Console.WriteLine($"   Message Prefix: {demoOptions.MessagePrefix}");
Console.WriteLine($"   Batch Size: {demoOptions.BatchSize}");
Console.WriteLine($"   Topic: {demoOptions.Topic}");
Console.WriteLine();

// Setup cancellation
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\n\n🛑 Shutdown signal received...");
};

IPublisher<DemoMessage>? publisher = null;
MessagePublisherService<DemoMessage>? publisherService = null;
SimpleHttpClientFactory? httpClientFactory = null;

try
{
    // Create HttpClientFactory for SchemaRegistry
    httpClientFactory = new SimpleHttpClientFactory();
    var schemaRegistryClientFactory = new SchemaRegistryClientFactory(httpClientFactory, schemaRegistryOptions);
    
    // Create PublisherFactory
    var publisherFactory = new PublisherFactory<DemoMessage>(schemaRegistryClientFactory);
    
    // Create PublisherOptions
    var publisherOptions = new PublisherOptions(
        MessageBrokerConnectionUri: new Uri($"messageBroker://{brokerOptions.Host}:{brokerOptions.Port}"),
        SchemaRegistryConnectionUri: schemaRegistryOptions.BaseAddress,
        SchemaRegistryTimeout: schemaRegistryOptions.Timeout,
        Topic: demoOptions.Topic ?? "default",
        MaxPublisherQueueSize: brokerOptions.MaxQueueSize,
        MaxSendAttempts: 5,
        MaxRetryAttempts: 5,
        BatchMaxBytes: demoOptions.BatchMaxBytes > 0 ? demoOptions.BatchMaxBytes : 65536,
        BatchMaxDelay: demoOptions.BatchMaxDelay > TimeSpan.Zero ? demoOptions.BatchMaxDelay : TimeSpan.FromMilliseconds(1000));
    
    // Create publisher
    publisher = publisherFactory.CreatePublisher(publisherOptions);
    
    // Create publishing service
    publisherService = new MessagePublisherService<DemoMessage>(publisher!, demoOptions);
    
    // Create SubscriberFactory
    var schemaRegistryClient = schemaRegistryClientFactory.Create();
    var subscriberFactory = new SubscriberFactory<DemoMessage>(schemaRegistryClient);
    
    // Create subscriber options
    var subscriberOptions = new SubscriberOptions
    {
        MessageBrokerConnectionUri = new Uri($"messageBroker://{brokerOptions.Host}:{brokerOptions.Port}"),
        SchemaRegistryConnectionUri = schemaRegistryOptions.BaseAddress,
        SchemaRegistryTimeout = schemaRegistryOptions.Timeout,
        Topic = demoOptions.Topic ?? "default",
        PollInterval = TimeSpan.FromMilliseconds(100),
        MaxRetryAttempts = 3
    };
    
    // Create subscriber
    var subscriber = subscriberFactory.CreateSubscriber(subscriberOptions, async (message) =>
    {
        Console.WriteLine($"📨 Subscriber otrzymał: Id={message.Id}, Content={message.Content}, Type={message.MessageType}");
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
finally
{
    // Cleanup
    if (publisherService is IAsyncDisposable serviceDisposable)
    {
        await serviceDisposable.DisposeAsync();
    }
    if (publisher != null && publisher is IAsyncDisposable publisherDisposable)
    {
        await publisherDisposable.DisposeAsync();
    }
    httpClientFactory?.Dispose();
}

Console.WriteLine("\n👋 PubSub Demo finished.");
return 0;
