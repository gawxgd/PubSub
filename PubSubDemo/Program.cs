using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
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
var schemaRegistryUrl = configuration.GetSection("SchemaRegistry:BaseAddress").Value 
    ?? Environment.GetEnvironmentVariable("PUBSUB_SchemaRegistry__BaseAddress") 
    ?? "http://localhost:8081";
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

// Calculate effective batch settings for display
// Use larger batches to avoid issues with batch length reading
// Minimum batch size should be at least 4KB to ensure proper batching
var batchMaxBytes = demoOptions.BatchMaxBytes > 0 
    ? Math.Min(Math.Max(demoOptions.BatchMaxBytes, 4096), 1048576) 
    : 16384; // Default to 16KB for more reliable batching
var batchMaxDelay = demoOptions.BatchMaxDelay > TimeSpan.Zero 
    ? TimeSpan.FromMilliseconds(Math.Min(Math.Max(demoOptions.BatchMaxDelay.TotalMilliseconds, 100), 30000))
    : TimeSpan.FromMilliseconds(1000); // Default to 1 second

// Ensure batch delay is at least 100ms to allow messages to accumulate
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

// Setup cancellation
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\n\nShutdown signal received...");
};

IPublisher<DemoMessage>? publisher = null;
MessagePublisherService<DemoMessage>? publisherService = null;
SimpleHttpClientFactory? httpClientFactory = null;

try
{
    // Register schema for DemoMessage first (using a separate HttpClient)
    var topic = demoOptions.Topic ?? "default";
    Console.WriteLine($"Rejestrowanie schematu dla topiku: {topic}...");
    try
    {
        await RegisterSchemaForDemoMessage(schemaRegistryOptions, topic);
        Console.WriteLine($"Schemat zarejestrowany pomyślnie!\n");
    }
    catch (Exception ex) when (ex.Message.Contains("actively refused") || ex.Message.Contains("connection") || ex.Message.Contains("SchemaRegistry"))
    {
        Console.WriteLine($"\nBłąd Schema Registry: {ex.Message}");
        Console.WriteLine("\nWskazówka: Upewnij się, że Schema Registry jest uruchomiony.");
        Console.WriteLine($"   Schema Registry powinien nasłuchiwać na {schemaRegistryOptions.BaseAddress}");
        Console.WriteLine("\n   Aby uruchomić Schema Registry:");
        Console.WriteLine("   cd SchemaRegistry/src");
        Console.WriteLine("   $env:ASPNETCORE_URLS='http://localhost:8081'");
        Console.WriteLine("   dotnet run");
        Console.WriteLine("\n   Lub w nowym terminalu:");
        Console.WriteLine("   cd SchemaRegistry/src");
        Console.WriteLine("   dotnet run --urls http://localhost:8081");
        return 1;
    }
    
    // Create HttpClientFactory for SchemaRegistry
    httpClientFactory = new SimpleHttpClientFactory();
    var schemaRegistryClientFactory = new SchemaRegistryClientFactory(httpClientFactory, schemaRegistryOptions);
    
    // Create PublisherFactory
    var publisherFactory = new PublisherFactory<DemoMessage>(schemaRegistryClientFactory);
    
    // Create PublisherOptions
    // Use the batch settings calculated above
    var publisherOptions = new PublisherOptions(
        MessageBrokerConnectionUri: new Uri($"messageBroker://{brokerOptions.Host}:{brokerOptions.Port}"),
        SchemaRegistryConnectionUri: schemaRegistryOptions.BaseAddress,
        SchemaRegistryTimeout: schemaRegistryOptions.Timeout,
        Topic: demoOptions.Topic ?? "default",
        MaxPublisherQueueSize: brokerOptions.MaxQueueSize,
        MaxSendAttempts: 5,
        MaxRetryAttempts: 5,
        BatchMaxBytes: batchMaxBytes,
        BatchMaxDelay: batchMaxDelay);
    
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
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(message.Timestamp).ToString("yyyy-MM-dd HH:mm:ss");
        Console.WriteLine($"Subscriber otrzymał: Id={message.Id}, Timestamp={timestamp}, Content={message.Content}, Type={message.MessageType}");
        await Task.CompletedTask;
    });
    
    // IMPORTANT: If you see "Invalid magic number" or "Batch length cannot be zero" errors,
    // it's because Subscriber uses BitConverter instead of BinaryPrimitives to read batch length.
    // This causes incorrect batch length reading and wrong data offset.
    // Fix needed in: Subscriber/src/Outbound/Adapter/TcpSubscriberConnection.cs line 159
    // Change: BitConverter.ToUInt32 -> BinaryPrimitives.ReadUInt32LittleEndian
    // Also fix lines 158 and 160 for baseOffset and lastOffset
    
    // Start everything
    Console.WriteLine("Uruchamianie Publisher i Subscriber...\n");
    
    try
    {
        // MessagePublisherService.StartAsync() already calls CreateConnection() internally
        await publisherService.StartAsync(cts.Token);
        await subscriber.StartConnectionAsync();
        await subscriber.StartMessageProcessingAsync();
        
        Console.WriteLine("Wszystko działa! Sprawdź frontend na http://localhost:3000");
        Console.WriteLine("   Naciśnij Ctrl+C aby zatrzymać.\n");
        
        // Wait for cancellation
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

Console.WriteLine("\nPubSub Demo finished.");
return 0;

static async Task RegisterSchemaForDemoMessage(
    SchemaRegistryClientOptions options,
    string topic)
{
    // Avro schema for DemoMessage
    // Note: Using long for Timestamp instead of timestamp-millis logical type
    // because Avro.Reflect doesn't automatically handle DateTimeOffset conversion
    const string demoMessageSchema = """
    {
      "type": "record",
      "name": "DemoMessage",
      "namespace": "PubSubDemo.Services",
      "fields": [
        { "name": "Id", "type": "int" },
        { "name": "Timestamp", "type": "long" },
        { "name": "Content", "type": "string" },
        { "name": "Source", "type": "string" },
        { "name": "MessageType", "type": "string" }
      ]
    }
    """;

    try
    {
        using var httpClient = new HttpClient();
        httpClient.BaseAddress = options.BaseAddress;
        httpClient.Timeout = options.Timeout;

        var endpoint = $"schema/topic/{topic}";
        var requestBody = new { Schema = demoMessageSchema };
        var json = JsonSerializer.Serialize(requestBody);
        var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
        
        var response = await httpClient.PostAsync(endpoint, content);
        
        if (response.IsSuccessStatusCode)
        {
            var result = await response.Content.ReadFromJsonAsync<JsonElement>();
            var schemaId = result.GetProperty("id").GetInt32();
            Console.WriteLine($"   Schema ID: {schemaId}");
        }
        else if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
        {
            Console.WriteLine("   Schemat już istnieje (Conflict) - używam istniejącego");
        }
        else
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            Console.WriteLine($"   Ostrzeżenie: Nie udało się zarejestrować schematu: {response.StatusCode}");
            Console.WriteLine($"   {errorContent}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"   Ostrzeżenie: Błąd podczas rejestracji schematu: {ex.Message}");
        Console.WriteLine("   Próba kontynuacji - schemat może już istnieć");
    }
}
