using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Confluent.SchemaRegistry;
using LoggerLib.Outbound.Adapter;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Infrastructure;
using PerformanceTests.Models;
using PerformanceTests.Scenarios;
using Publisher.Configuration;
using Publisher.Configuration.Options;
using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Outbound.SchemaRegistryClient;
using Subscriber.Configuration;
using Subscriber.Configuration.Options;
using Subscriber.Domain;

// Initialize logger
var logger = new ConsoleLogger();
AutoLoggerFactory.Initialize(logger);

Console.WriteLine("╔════════════════════════════════════════════╗");
Console.WriteLine("║   PubSub Performance Tests with NBomber   ║");
Console.WriteLine("╚════════════════════════════════════════════╝");
Console.WriteLine();

// Configuration - PubSub System
var brokerHost = Environment.GetEnvironmentVariable("BROKER_HOST") ?? "127.0.0.1";
var brokerPort = int.Parse(Environment.GetEnvironmentVariable("BROKER_PORT") ?? "9096");
var schemaRegistryUrl = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://localhost:8081";
var topic = Environment.GetEnvironmentVariable("TOPIC") ?? "performance-test";

// Configuration - Kafka (optional, defaults to 127.0.0.1 to force IPv4)
var kafkaBootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "127.0.0.1:9092";
var kafkaSchemaRegistryUrl = Environment.GetEnvironmentVariable("KAFKA_SCHEMA_REGISTRY_URL") ?? "http://localhost:8081";
var kafkaTopic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? $"{topic}-kafka";
var kafkaConsumerGroupId = Environment.GetEnvironmentVariable("KAFKA_CONSUMER_GROUP_ID") ?? "performance-test-consumer-group";
var enableKafkaTests = Environment.GetEnvironmentVariable("ENABLE_KAFKA_TESTS")?.ToLower() == "true";

var brokerUri = new Uri($"messageBroker://{brokerHost}:{brokerPort}");
var schemaRegistryUri = new Uri(schemaRegistryUrl);

Console.WriteLine("Configuration - PubSub System:");
Console.WriteLine($"   Broker: {brokerUri}");
Console.WriteLine($"   Schema Registry: {schemaRegistryUri}");
Console.WriteLine($"   Topic: {topic}");
Console.WriteLine();
Console.WriteLine("Configuration - Kafka:");
Console.WriteLine($"   Bootstrap Servers: {kafkaBootstrapServers}");
Console.WriteLine($"   Schema Registry: {kafkaSchemaRegistryUrl}");
Console.WriteLine($"   Topic: {kafkaTopic}");
Console.WriteLine($"   Consumer Group: {kafkaConsumerGroupId}");
Console.WriteLine($"   Kafka Tests Enabled: {enableKafkaTests}");
Console.WriteLine();

// Check if broker is available
Console.WriteLine($"Checking broker availability at {brokerHost}:{brokerPort}...");
try
{
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    using var tcpClient = new System.Net.Sockets.TcpClient();
    
    try
    {
        // Connect with timeout
        var connectTask = tcpClient.ConnectAsync(brokerHost, brokerPort);
        await connectTask.WaitAsync(cts.Token);
        Console.WriteLine($"MessageBroker is available at {brokerHost}:{brokerPort}");
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"ERROR: Timeout connecting to MessageBroker at {brokerHost}:{brokerPort}");
        Console.WriteLine();
        Console.WriteLine("To start MessageBroker and SchemaRegistry:");
        Console.WriteLine("   1. Using Docker Compose:");
        Console.WriteLine("      docker compose up -d messagebroker schemaregistry");
        Console.WriteLine();
        Console.WriteLine("   2. Or manually (PowerShell):");
        Console.WriteLine("      # Terminal 1 - MessageBroker:");
        Console.WriteLine("      cd MessageBroker\\src");
        Console.WriteLine("      dotnet run");
        Console.WriteLine();
        Console.WriteLine("      # Terminal 2 - SchemaRegistry:");
        Console.WriteLine("      cd SchemaRegistry\\src");
        Console.WriteLine("      $env:ASPNETCORE_URLS='http://localhost:8081'");
        Console.WriteLine("      dotnet run");
        Console.WriteLine();
        Console.WriteLine("   Then run the performance tests again.");
        Environment.Exit(1);
    }
    catch (System.Net.Sockets.SocketException ex)
    {
        Console.WriteLine($"ERROR: Cannot connect to MessageBroker at {brokerHost}:{brokerPort}");
        Console.WriteLine($"   Socket Error: {ex.SocketErrorCode} - {ex.Message}");
        Console.WriteLine();
        Console.WriteLine("Troubleshooting:");
        Console.WriteLine($"   - Check if MessageBroker is running: netstat -an | findstr {brokerPort}");
        Console.WriteLine($"   - Try connecting manually: Test-NetConnection -ComputerName {brokerHost} -Port {brokerPort}");
        Console.WriteLine();
        Console.WriteLine("Please ensure MessageBroker is running before starting performance tests.");
        Environment.Exit(1);
    }
}
catch (Exception ex)
{
    Console.WriteLine($"ERROR: Unexpected error checking MessageBroker at {brokerHost}:{brokerPort}");
    Console.WriteLine($"   Error: {ex.Message}");
    Console.WriteLine($"   Type: {ex.GetType().Name}");
    Console.WriteLine();
    Console.WriteLine("Please ensure MessageBroker is running before starting performance tests.");
    Environment.Exit(1);
}

// Check if SchemaRegistry is available
Console.WriteLine("Checking SchemaRegistry availability...");
try
{
    using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
    var response = await httpClient.GetAsync($"{schemaRegistryUrl}/swagger");
    if (response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.NotFound)
    {
        Console.WriteLine("SchemaRegistry is available");
    }
    else
    {
        throw new HttpRequestException($"SchemaRegistry returned status {response.StatusCode}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"WARNING: Cannot connect to SchemaRegistry at {schemaRegistryUrl}");
    Console.WriteLine($"   Error: {ex.Message}");
    Console.WriteLine("   Performance tests will try to register schema automatically.");
}
Console.WriteLine();

// Register schema for TestMessage - MUST succeed before creating publishers
Console.WriteLine($"Registering schema for topic: {topic}...");
try
{
    await RegisterSchemaForTestMessage(schemaRegistryUri, topic);
    Console.WriteLine($"Schema registered successfully!\n");
    
    // Verify schema is available by trying to get it
    await Task.Delay(TimeSpan.FromMilliseconds(200)); // Small delay to ensure schema is committed
    Console.WriteLine($"Verifying schema is available...");
    using var verifyClient = new HttpClient();
    verifyClient.BaseAddress = schemaRegistryUri;
    verifyClient.Timeout = TimeSpan.FromSeconds(5);
    var verifyResponse = await verifyClient.GetAsync($"schema/topic/{topic}");
    if (verifyResponse.IsSuccessStatusCode)
    {
        var verifyResult = await verifyResponse.Content.ReadFromJsonAsync<JsonElement>();
        var schemaId = verifyResult.GetProperty("id").GetInt32();
        Console.WriteLine($"Schema verified - ID: {schemaId}\n");
    }
    else
    {
        Console.WriteLine($"WARNING: Schema verification failed: {verifyResponse.StatusCode}");
        Console.WriteLine("   Tests may fail if Publisher cannot get schema.\n");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"ERROR: Failed to register schema: {ex.GetType().Name} - {ex.Message}");
    if (ex.InnerException != null)
    {
        Console.WriteLine($"   Inner: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
    }
    Console.WriteLine();
    Console.WriteLine("CRITICAL: Schema registration failed! Tests will likely fail.");
    Console.WriteLine("   Please ensure SchemaRegistry is running and accessible.");
    Console.WriteLine("   Continuing anyway - tests will show the actual error...");
    Console.WriteLine();
}

// Setup factories
var httpClientFactory = new SimpleHttpClientFactory();
var schemaRegistryOptions = new SchemaRegistryClientOptions(schemaRegistryUri, TimeSpan.FromSeconds(10));
var schemaRegistryClientFactory = new SchemaRegistryClientFactory(httpClientFactory, schemaRegistryOptions);
var publisherFactory = new PublisherFactory<TestMessage>(schemaRegistryClientFactory);
var schemaRegistryClient = schemaRegistryClientFactory.Create();
var subscriberFactory = new SubscriberFactory<TestMessage>(schemaRegistryClient);

// Publisher options
var publisherOptions = new PublisherOptions(
    MessageBrokerConnectionUri: brokerUri,
    SchemaRegistryConnectionUri: schemaRegistryUri,
    SchemaRegistryTimeout: TimeSpan.FromSeconds(10),
    Topic: topic,
    MaxPublisherQueueSize: 10000,
    MaxSendAttempts: 3,
    MaxRetryAttempts: 3,
    BatchMaxBytes: 65536,
    BatchMaxDelay: TimeSpan.FromMilliseconds(100));

// Subscriber options
var subscriberOptions = new SubscriberOptions
{
    MessageBrokerConnectionUri = brokerUri,
    SchemaRegistryConnectionUri = schemaRegistryUri,
    SchemaRegistryTimeout = TimeSpan.FromSeconds(10),
    Topic = topic,
    PollInterval = TimeSpan.FromMilliseconds(50),
    MaxRetryAttempts = 3
    // MaxQueueSize is read-only with default value of 65536
};

// Create PubSub scenarios
var scenarios = new List<ScenarioProps>();

ScenarioProps publisherPerformanceScenario =
    PublisherPerformanceScenario.Create(publisherFactory, publisherOptions);
scenarios.Add(publisherPerformanceScenario);

ScenarioProps endToEndScenario20 =
    EndToEndPerformanceScenario.Create(
        20,
        60,
        publisherFactory,
        publisherOptions,
        subscriberFactory,
        subscriberOptions);

ScenarioProps endToEndScenario40 =
    EndToEndPerformanceScenario.Create(
        40,
        60,
        publisherFactory,
        publisherOptions,
        subscriberFactory,
        subscriberOptions);

ScenarioProps endToEndScenario100 =
    EndToEndPerformanceScenario.Create(
        100,
        60,
        publisherFactory,
        publisherOptions,
        subscriberFactory,
        subscriberOptions);

ScenarioProps endToEndScenario200 =
    EndToEndPerformanceScenario.Create(
        200,
        60,
        publisherFactory,
        publisherOptions,
        subscriberFactory,
        subscriberOptions);

scenarios.Add(endToEndScenario20);
scenarios.Add(endToEndScenario40);
scenarios.Add(endToEndScenario100);
scenarios.Add(endToEndScenario200);
//scenarios.Add(endToEndScenario10000);

ScenarioProps publisherLatencyScenario =
    PublisherLatencyScenario.Create(publisherFactory, publisherOptions);
scenarios.Add(publisherLatencyScenario);

ScenarioProps fanOutScenario5 = FanOutPerformanceScenario.Create(
        5,
        publisherFactory,
        publisherOptions,
        subscriberFactory,
        subscriberOptions);

ScenarioProps fanOutScenario10 = FanOutPerformanceScenario.Create(
        10,
        publisherFactory,
        publisherOptions,
        subscriberFactory,
        subscriberOptions);

ScenarioProps fanOutScenario20 = FanOutPerformanceScenario.Create(
        20,
        publisherFactory,
        publisherOptions,
        subscriberFactory,
        subscriberOptions);

// Create Kafka scenarios if enabled
if (enableKafkaTests)
{
    Console.WriteLine("\nPreparing Kafka tests...");
    Console.WriteLine("   Note: Using JSON serialization (no Schema Registry required)\n");

    ScenarioProps kafkaPublisherPerformanceScenario =
        KafkaPublisherPerformanceScenario.Create(kafkaBootstrapServers, kafkaSchemaRegistryUrl, kafkaTopic);
    scenarios.Add(kafkaPublisherPerformanceScenario);

    ScenarioProps kafkaEndToEndScenario =
        KafkaEndToEndPerformanceScenario.Create(kafkaBootstrapServers, kafkaSchemaRegistryUrl, kafkaTopic, kafkaConsumerGroupId);
    scenarios.Add(kafkaEndToEndScenario);

    ScenarioProps kafkaPublisherLatencyScenario =
        KafkaPublisherLatencyScenario.Create(kafkaBootstrapServers, kafkaSchemaRegistryUrl, kafkaTopic);
    scenarios.Add(kafkaPublisherLatencyScenario);

    ScenarioProps kafkaFanOutScenario5 = KafkaFanOutPerformanceScenario.Create(
        5,
        kafkaBootstrapServers,
        kafkaTopic,
        "fanout");

ScenarioProps kafkaFanOutScenario10 = KafkaFanOutPerformanceScenario.Create(
        10,
        kafkaBootstrapServers,
        kafkaTopic,
        "fanout");

ScenarioProps kafkaFanOutScenario20 = KafkaFanOutPerformanceScenario.Create(
        20,
        kafkaBootstrapServers,
        kafkaTopic,
        "fanout");


    scenarios.Add(kafkaFanOutScenario5);
scenarios.Add(kafkaFanOutScenario10);
scenarios.Add(kafkaFanOutScenario20);

    Console.WriteLine("Kafka scenarios added.\n");
}

// Run tests with progress logging
Console.WriteLine("\nStarting performance tests...");
if (enableKafkaTests)
{
    Console.WriteLine("   Estimated duration: ~2 minutes (PubSub + Kafka)");
    Console.WriteLine("   PubSub Tests:");
    Console.WriteLine("   - Publisher Throughput: ~18s (3s warm-up + 15s test)");
    Console.WriteLine("   - End-to-End Throughput: ~23s (3s warm-up + 20s test)");
    Console.WriteLine("   - Publisher Latency: ~18s (3s warm-up + 15s test)");
    Console.WriteLine("   Kafka Tests:");
    Console.WriteLine("   - Kafka Publisher Throughput: ~18s (3s warm-up + 15s test)");
    Console.WriteLine("   - Kafka End-to-End Throughput: ~23s (3s warm-up + 20s test)");
    Console.WriteLine("   - Kafka Publisher Latency: ~18s (3s warm-up + 15s test)");
}
else
{
    Console.WriteLine("   Estimated duration: ~1 minute");
    Console.WriteLine("   - Publisher Throughput: ~18s (3s warm-up + 15s test)");
    Console.WriteLine("   - End-to-End Throughput: ~23s (3s warm-up + 20s test)");
    Console.WriteLine("   - Publisher Latency: ~18s (3s warm-up + 15s test)");
    Console.WriteLine();
    Console.WriteLine("   Note: Set ENABLE_KAFKA_TESTS=true to include Kafka performance tests");
}
Console.WriteLine();

var startTime = DateTime.UtcNow;

NBomberRunner
    .RegisterScenarios(scenarios.ToArray())
    .WithReportFolder("reports")
    .Run();

var duration = DateTime.UtcNow - startTime;
Console.WriteLine($"\nTotal test duration: {duration.TotalSeconds:F1} seconds");

// Cleanup
httpClientFactory.Dispose();
if (schemaRegistryClient is IDisposable disposable)
{
    disposable.Dispose();
}

Console.WriteLine("\nPerformance tests completed!");
Console.WriteLine("Check reports folder for detailed results.");

static async Task RegisterSchemaForTestMessage(Uri schemaRegistryBaseAddress, string topic)
{
    const string testMessageSchema = """
    {
      "type": "record",
      "name": "TestMessage",
      "namespace": "PerformanceTests.Models",
      "fields": [
        { "name": "Id", "type": "int" },
        { "name": "Timestamp", "type": "long" },
        { "name": "Content", "type": "string" },
        { "name": "Source", "type": "string" },
        { "name": "SequenceNumber", "type": "long" }
      ]
    }
    """;

    try
    {
        using var httpClient = new HttpClient();
        httpClient.BaseAddress = schemaRegistryBaseAddress;
        httpClient.Timeout = TimeSpan.FromSeconds(10);

        var endpoint = $"schema/topic/{topic}";
        var requestBody = new { Schema = testMessageSchema };
        var json = JsonSerializer.Serialize(requestBody);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        var response = await httpClient.PostAsync(endpoint, content);
        
        if (response.IsSuccessStatusCode)
        {
            var result = await response.Content.ReadFromJsonAsync<JsonElement>();
            var schemaId = result.GetProperty("id").GetInt32();
            Console.WriteLine($"   Schema ID: {schemaId}");
        }
        else if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
        {
            Console.WriteLine("   Schema already exists (Conflict) - using existing");
        }
        else
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            throw new HttpRequestException($"Failed to register schema: {response.StatusCode} - {errorContent}");
        }
    }
    catch (Exception ex)
    {
        throw new InvalidOperationException($"Failed to register schema for TestMessage: {ex.Message}", ex);
    }
}

static async Task RegisterSchemaInKafkaSchemaRegistry(string schemaRegistryUrl, string topic)
{
    // JSON Schema for TestMessage
    const string testMessageJsonSchema = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "Id": {
          "type": "integer"
        },
        "Timestamp": {
          "type": "integer",
          "format": "int64"
        },
        "Content": {
          "type": "string"
        },
        "Source": {
          "type": "string"
        },
        "SequenceNumber": {
          "type": "integer",
          "format": "int64"
        }
      },
      "required": ["Id", "Timestamp", "Content", "Source", "SequenceNumber"]
    }
    """;

    try
    {
        // Confluent Schema Registry uses different API format
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryUrl };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        // Register schema for the topic (subject name is typically {topic}-value)
        var subjectName = $"{topic}-value";
        var schema = new Confluent.SchemaRegistry.Schema(testMessageJsonSchema, SchemaType.Json);
        
        var schemaId = await schemaRegistry.RegisterSchemaAsync(subjectName, schema);
        Console.WriteLine($"   Kafka JSON Schema ID: {schemaId}");
    }
    catch (Exception ex)
    {
        // If schema already exists, that's OK
        if (ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase) ||
            ex.Message.Contains("409", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine(" Kafka schema already exists - using existing");
        }
        else
        {
            throw new InvalidOperationException($"Failed to register Kafka schema for TestMessage: {ex.Message}", ex);
        }
    }
}