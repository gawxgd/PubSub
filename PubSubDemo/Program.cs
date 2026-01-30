using System.Net.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Http;
using Publisher.Configuration;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using Publisher.Outbound.Exceptions;
using PubSubDemo.Configuration;
using PubSubDemo.Infrastructure;
using PubSubDemo.Services;
using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Domain.Port.SchemaRegistryClient;
using Shared.Outbound.SchemaRegistryClient;
using Subscriber.Configuration;
using Subscriber.Configuration.Options;
using Subscriber.Outbound.Exceptions;
using LoggerLib.Outbound.Adapter;

var logger = new LoggerLib.Outbound.Adapter.ConsoleLogger();
AutoLoggerFactory.Initialize(logger);

Console.WriteLine("╔════════════════════════════════════════════╗");
Console.WriteLine("║   PubSub Full Demo - Publisher & Subscriber ║");
Console.WriteLine("╚════════════════════════════════════════════╝");
Console.WriteLine();

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", false, true)
    .AddEnvironmentVariables("PUBSUB_")
    .AddCommandLine(args)
    .Build();

var brokerOptions = configuration.GetSection("Broker").Get<BrokerOptions>() ?? new BrokerOptions();
var demoOptions = configuration.GetSection("Demo").Get<DemoOptions>() ?? new DemoOptions();
var schemaRegistryUrl = configuration.GetSection("SchemaRegistry:BaseAddress").Value 
    ?? Environment.GetEnvironmentVariable("PUBSUB_SchemaRegistry__BaseAddress") 
    ?? "http:
var schemaRegistryTimeout = configuration.GetSection("SchemaRegistry:Timeout").Value != null
    ? TimeSpan.Parse(configuration.GetSection("SchemaRegistry:Timeout").Value!)
    : TimeSpan.FromSeconds(10);
var schemaRegistryOptions = new SchemaRegistryClientOptions(
    new Uri(schemaRegistryUrl),
    schemaRegistryTimeout);

Console.WriteLine("Broker Configuration:");
Console.WriteLine($"   Host: {brokerOptions.Host}");
Console.WriteLine($"   Port: {brokerOptions.Port}");
Console.WriteLine($"   Queue Size: {brokerOptions.MaxQueueSize}");
Console.WriteLine();

Console.WriteLine("Schema Registry Configuration:");
Console.WriteLine($"   Base Address: {schemaRegistryOptions.BaseAddress}");
Console.WriteLine($"   Timeout: {schemaRegistryOptions.Timeout}");
Console.WriteLine();

var batchMaxBytes = demoOptions.BatchMaxBytes > 0 
    ? Math.Min(Math.Max(demoOptions.BatchMaxBytes, 4096), 1048576) 
    : 16384;
var batchMaxDelay = demoOptions.BatchMaxDelay > TimeSpan.Zero 
    ? TimeSpan.FromMilliseconds(Math.Min(Math.Max(demoOptions.BatchMaxDelay.TotalMilliseconds, 100), 30000))
    : TimeSpan.FromMilliseconds(1000);

if (batchMaxDelay.TotalMilliseconds < 100)
{
    batchMaxDelay = TimeSpan.FromMilliseconds(100);
    Console.WriteLine($"BatchMaxDelay adjusted to minimum 100ms for proper batching");
}

Console.WriteLine("⚙Demo Configuration:");
Console.WriteLine($"   Message Interval: {demoOptions.MessageInterval}ms");
Console.WriteLine($"   Message Prefix: {demoOptions.MessagePrefix}");
Console.WriteLine($"   Batch Size: {demoOptions.BatchSize}");
Console.WriteLine($"   Topic: {demoOptions.Topic}");
Console.WriteLine($"   Batch Max Bytes: {demoOptions.BatchMaxBytes} (effective: {batchMaxBytes})");
Console.WriteLine($"   Batch Max Delay: {demoOptions.BatchMaxDelay} (effective: {batchMaxDelay.TotalMilliseconds}ms)");
Console.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\n\nShutdown signal received...");
};

MessagePublisherService<DemoMessage>? publisherService = null;
SimpleHttpClientFactory? httpClientFactory = null;

var topicsToUse = (demoOptions.Topics ?? Array.Empty<string>())
    .Where(t => !string.IsNullOrWhiteSpace(t))
    .Distinct(StringComparer.OrdinalIgnoreCase)
    .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
    .ToArray();

if (topicsToUse.Length == 0)
{
    topicsToUse = new[] { demoOptions.Topic ?? "default" };
}

Console.WriteLine($"Using topics: {string.Join(", ", topicsToUse)}\n");

try
{
    httpClientFactory = new SimpleHttpClientFactory();
    var schemaRegistryClientFactory = new SchemaRegistryClientFactory(httpClientFactory, schemaRegistryOptions);
    
    var publisherFactory = new PublisherFactory<DemoMessage>(schemaRegistryClientFactory);
    
    var publishers = topicsToUse.Select(t =>
    {
        var publisherOptions = new PublisherOptions(
            MessageBrokerConnectionUri: new Uri($"messageBroker:
            SchemaRegistryConnectionUri: schemaRegistryOptions.BaseAddress,
            SchemaRegistryTimeout: schemaRegistryOptions.Timeout,
            Topic: t,
            MaxPublisherQueueSize: brokerOptions.MaxQueueSize,
            MaxSendAttempts: 5,
            MaxRetryAttempts: 5,
            BatchMaxBytes: batchMaxBytes,
            BatchMaxDelay: batchMaxDelay);
        return (Topic: t, Publisher: publisherFactory.CreatePublisher(publisherOptions));
    }).ToArray();

    publisherService = new MessagePublisherService<DemoMessage>(publishers, demoOptions);
    
    var schemaRegistryClient = schemaRegistryClientFactory.Create();
    var subscriberFactory = new SubscriberFactory<DemoMessage>(schemaRegistryClient);
    
    var subscriberOptions = new SubscriberOptions(
        MessageBrokerConnectionUri: new Uri($"messageBroker:
        SchemaRegistryConnectionUri: schemaRegistryOptions.BaseAddress,
        Host: brokerOptions.Host,
        Port: brokerOptions.SubscriberPort,
        Topic: demoOptions.Topic ?? "default",
        MinMessageLength: 0,
        MaxMessageLength: int.MaxValue,
        MaxQueueSize: 65536,
        PollInterval: TimeSpan.FromMilliseconds(100),
        SchemaRegistryTimeout: schemaRegistryOptions.Timeout,
        MaxRetryAttempts: 3
    );

    static async Task SubscriberMessageHandler(DemoMessage message)
    {
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(message.Timestamp).ToString("yyyy-MM-dd HH:mm:ss");
        Console.WriteLine($"Subscriber otrzymał: Id={message.Id}, Timestamp={timestamp}, Content={message.Content}, Type={message.MessageType}");
        await Task.CompletedTask;
    }

    var subscriber = subscriberFactory.CreateSubscriber(subscriberOptions, SubscriberMessageHandler);

    Console.WriteLine("Uruchamianie Publisher i Subscriber...\n");
    
    try
    {
        await publisherService.StartAsync(cts.Token);
        await subscriber.StartConnectionAsync();
        await subscriber.StartMessageProcessingAsync();
        
        Console.WriteLine("Wszystko działa! Sprawdź frontend na http:
        Console.WriteLine("   Naciśnij Ctrl+C aby zatrzymać.\n");
        
        await Task.Delay(Timeout.Infinite, cts.Token);
    }
    catch (PublisherException ex)
    {
        Console.WriteLine($"\nBłąd Publisher: {ex.Message}");
        Console.WriteLine("\nWskazówka: Upewnij się, że:");
        Console.WriteLine("   1. MessageBroker jest uruchomiony");
        Console.WriteLine($"   2. MessageBroker nasłuchuje na {brokerOptions.Host}:{brokerOptions.Port}");
        Console.WriteLine("   3. Port nie jest zablokowany przez firewall");
        Console.WriteLine("\n   Aby uruchomić MessageBroker:");
        Console.WriteLine("   cd MessageBroker/src");
        Console.WriteLine("   dotnet run");
        return 1;
    }
    catch (Subscriber.Outbound.Exceptions.SubscriberConnectionException ex)
    {
        Console.WriteLine($"\nBłąd Subscriber: {ex.Message}");
        Console.WriteLine("\nWskazówka: Upewnij się, że MessageBroker jest uruchomiony.");
        return 1;
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Graceful shutdown complete.");
}
finally
{
    if (publisherService is IAsyncDisposable serviceDisposable)
    {
        await serviceDisposable.DisposeAsync();
    }
    httpClientFactory?.Dispose();
}

Console.WriteLine("\nPubSub Demo finished.");
return 0;
