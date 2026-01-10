using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
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

// Get current settings
ThreadPool.GetMinThreads(out int minWorker, out int minIOC);
Console.WriteLine($"Current ThreadPool: minWorker={minWorker}, minIOC={minIOC}");

// Set higher minimum threads (adjust based on your load)
// For 5000 msg/s with 10 topics, start with 100-200 threads
ThreadPool.SetMinThreads(200, 200);

ThreadPool.GetMinThreads(out minWorker, out minIOC);
Console.WriteLine($"Updated ThreadPool: minWorker={minWorker}, minIOC={minIOC}");


Console.WriteLine("╔═══════════════════════════════════════════════════╗");
Console.WriteLine("║   PubSub LONG-RUN Performance Test (2 Hours)     ║");
Console.WriteLine("╚═══════════════════════════════════════════════════╝");
Console.WriteLine();

// Configuration - PubSub System
var brokerHost = Environment.GetEnvironmentVariable("BROKER_HOST") ?? "127.0.0.1";
var brokerPort = int.Parse(Environment.GetEnvironmentVariable("BROKER_PORT") ?? "9096");
var schemaRegistryUrl = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://127.0.0.1:8081";

var brokerUri = new Uri($"messageBroker://{brokerHost}:{brokerPort}");
var schemaRegistryUri = new Uri(schemaRegistryUrl);

Console.WriteLine("Configuration:");
Console.WriteLine($"   Broker: {brokerUri}");
Console.WriteLine($"   Schema Registry: {schemaRegistryUri}");
Console.WriteLine();

// Check if broker is available
Console.WriteLine($"Checking broker availability at {brokerHost}:{brokerPort}...");
try
{
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    using var tcpClient = new System.Net.Sockets.TcpClient();
    
    try
    {
        var connectTask = tcpClient.ConnectAsync(brokerHost, brokerPort);
        await connectTask.WaitAsync(cts.Token);
        Console.WriteLine($"✓ MessageBroker is available at {brokerHost}:{brokerPort}");
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"✗ ERROR: Timeout connecting to MessageBroker at {brokerHost}:{brokerPort}");
        Console.WriteLine();
        Console.WriteLine("To start MessageBroker:");
        Console.WriteLine("   docker compose up -d messagebroker schemaregistry");
        Environment.Exit(1);
    }
    catch (System.Net.Sockets.SocketException ex)
    {
        Console.WriteLine($"✗ ERROR: Cannot connect to MessageBroker at {brokerHost}:{brokerPort}");
        Console.WriteLine($"   Socket Error: {ex.SocketErrorCode} - {ex.Message}");
        Environment.Exit(1);
    }
}
catch (Exception ex)
{
    Console.WriteLine($"✗ ERROR: Unexpected error checking MessageBroker: {ex.Message}");
    Environment.Exit(1);
}
Console.WriteLine();

// Check if SchemaRegistry is available
Console.WriteLine("Checking SchemaRegistry availability...");
try
{
    using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    var response = await httpClient.GetAsync($"{schemaRegistryUrl}/swagger");
    if (response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.NotFound)
    {
        Console.WriteLine($"✓ SchemaRegistry is available at {schemaRegistryUrl}");
    }
    else
    {
        throw new HttpRequestException($"SchemaRegistry returned status {response.StatusCode}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"✗ WARNING: Cannot connect to SchemaRegistry at {schemaRegistryUrl}");
    Console.WriteLine($"   Error: {ex.Message}");
    Console.WriteLine("   Tests may fail!");
}
Console.WriteLine();


Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine();

// Setup factories
var httpClientFactory = new SimpleHttpClientFactory();
var schemaRegistryOptions = new SchemaRegistryClientOptions(schemaRegistryUri, TimeSpan.FromSeconds(10));
var schemaRegistryClientFactory = new SchemaRegistryClientFactory(httpClientFactory, schemaRegistryOptions);
var publisherFactory = new PublisherFactory<TestMessage>(schemaRegistryClientFactory);
var schemaRegistryClient = schemaRegistryClientFactory.Create();
var subscriberFactory = new SubscriberFactory<TestMessage>(schemaRegistryClient);

// Publisher options template (topic will be set per scenario)
var publisherOptions = new PublisherOptions(
    MessageBrokerConnectionUri: brokerUri,
    SchemaRegistryConnectionUri: schemaRegistryUri,
    SchemaRegistryTimeout: TimeSpan.FromSeconds(10),
    Topic: string.Empty, // Will be set per topic
    MaxPublisherQueueSize: 10000,
    MaxSendAttempts: 3,
    MaxRetryAttempts: 3,
    BatchMaxBytes: 65536,
    BatchMaxDelay: TimeSpan.FromMilliseconds(100));

// Subscriber options template (topic will be set per scenario)
var subscriberOptions = new SubscriberOptions
{
    MessageBrokerConnectionUri = brokerUri,
    SchemaRegistryConnectionUri = schemaRegistryUri,
    SchemaRegistryTimeout = TimeSpan.FromSeconds(10),
    Topic = string.Empty, // Will be set per topic
    PollInterval = TimeSpan.FromMilliseconds(50),
    MaxRetryAttempts = 3
};


// Create long-run scenario
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine("CREATING LONG-RUN MULTI-TOPIC SCENARIO");
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine("Configuration:");
Console.WriteLine("   Topics: 10");
Console.WriteLine("   Publishers per topic: 10");
Console.WriteLine("   Subscribers per topic: 5");
Console.WriteLine("   Total rate: 5,000 msg/s");
Console.WriteLine("   Rate per topic: 500 msg/s");
Console.WriteLine("   Duration: 2 hours (7,200 seconds)");
Console.WriteLine("   Expected total messages: 36,000,000");
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine();

var scenarios = MultiTopicPeakPerformanceScenario.Create(
    publisherFactory,
    publisherOptions,
    subscriberFactory,
    subscriberOptions);


Console.WriteLine($"✓ Created {scenarios.Length} long-run scenarios");
Console.WriteLine();

// Run test
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine("STARTING LONG-RUN TEST");
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine($"Start time: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
Console.WriteLine($"Estimated end time: {DateTime.UtcNow.AddHours(2):yyyy-MM-dd HH:mm:ss} UTC");
Console.WriteLine();
Console.WriteLine("Press Ctrl+C to stop the test early.");
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine();

var startTime = DateTime.UtcNow;

NBomberRunner
    .RegisterScenarios(scenarios)
    .WithScenarioCompletionTimeout(TimeSpan.FromMinutes(5))
    .WithReportFolder("reports")
    .WithReportFileName("long_run_report")
    .Run();

var duration = DateTime.UtcNow - startTime;
var endTime = DateTime.UtcNow;

Console.WriteLine();
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine("TEST COMPLETED");
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine($"Start time:    {startTime:yyyy-MM-dd HH:mm:ss} UTC");
Console.WriteLine($"End time:      {endTime:yyyy-MM-dd HH:mm:ss} UTC");
Console.WriteLine($"Duration:      {duration.TotalHours:F2} hours ({duration.TotalSeconds:F0} seconds)");
Console.WriteLine($"Expected msgs: 36,000,000");
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine();

// Cleanup
httpClientFactory.Dispose();
if (schemaRegistryClient is IDisposable disposable)
{
    disposable.Dispose();
}

Console.WriteLine("✓ Performance test completed!");
Console.WriteLine("✓ Check 'reports' folder for detailed results.");
Console.WriteLine();

static async Task RegisterSchemaForTestMessage(Uri schemaRegistryBaseAddress, string topic)
{
    var cleanTopic = topic.Replace("schema/topic/", "").Trim('/');
    
    // UNIKALNY NAMESPACE dla każdego topicu!
    var schemaNamespace = $"PerformanceTests.Models.{cleanTopic.Replace("-", "_")}";
    
    var testMessageSchema = $$"""
    {
      "type": "record",
      "name": "TestMessage",
      "namespace": "{{schemaNamespace}}",
      "fields": [
        { "name": "Id", "type": "int" },
        { "name": "Timestamp", "type": "long" },
        { "name": "Content", "type": "string" },
        { "name": "Source", "type": "string" },
        { "name": "SequenceNumber", "type": "long" }
      ]
    }
    """;

    Console.WriteLine($"      [REGISTER] Topic: '{cleanTopic}'");
    Console.WriteLine($"      [REGISTER] Namespace: '{schemaNamespace}'");

    try
    {
        using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        
        var url = $"{schemaRegistryBaseAddress.ToString().TrimEnd('/')}/schema/topic/{cleanTopic}";
        Console.WriteLine($"      [REGISTER] URL: {url}");
        
        var requestBody = new { Schema = testMessageSchema };
        var json = JsonSerializer.Serialize(requestBody);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        var response = await httpClient.PostAsync(url, content);
        var responseContent = await response.Content.ReadAsStringAsync();
        
        Console.WriteLine($"      [REGISTER] Status: {response.StatusCode}");
        Console.WriteLine($"      [REGISTER] Response: {responseContent}");
        
        if (response.IsSuccessStatusCode)
        {
            var result = JsonSerializer.Deserialize<JsonElement>(responseContent);
            var schemaId = result.GetProperty("id").GetInt32();
            Console.WriteLine($"      ✓ Schema registered - ID: {schemaId}");
        }
        else if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
        {
            Console.WriteLine("      ✓ Schema already exists");
        }
        else
        {
            Console.WriteLine($"      ✗ Registration failed: {response.StatusCode}");
            throw new HttpRequestException($"Failed: {response.StatusCode} - {responseContent}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"      ✗ EXCEPTION: {ex.GetType().Name} - {ex.Message}");
        throw;
    }
}




