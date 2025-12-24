using System.Collections.Concurrent;
using System.Net.Http;
using System.Text;
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

// Configuration
var brokerHost = Environment.GetEnvironmentVariable("BROKER_HOST") ?? "localhost";
var brokerPort = int.Parse(Environment.GetEnvironmentVariable("BROKER_PORT") ?? "9096");
var schemaRegistryUrl = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://localhost:8081";
var topic = Environment.GetEnvironmentVariable("TOPIC") ?? "performance-test";

var brokerUri = new Uri($"messageBroker://{brokerHost}:{brokerPort}");
var schemaRegistryUri = new Uri(schemaRegistryUrl);

Console.WriteLine("📡 Configuration:");
Console.WriteLine($"   Broker: {brokerUri}");
Console.WriteLine($"   Schema Registry: {schemaRegistryUri}");
Console.WriteLine($"   Topic: {topic}");
Console.WriteLine();

// Check if broker is available
Console.WriteLine("🔍 Checking broker availability...");
try
{
    using var tcpClient = new System.Net.Sockets.TcpClient();
    var connectTask = tcpClient.ConnectAsync(brokerHost, brokerPort);
    var timeoutTask = Task.Delay(TimeSpan.FromSeconds(2));
    var completedTask = await Task.WhenAny(connectTask, timeoutTask);
    
    if (completedTask == timeoutTask)
    {
        Console.WriteLine("❌ ERROR: MessageBroker is not running!");
        Console.WriteLine();
        Console.WriteLine("📋 To start MessageBroker and SchemaRegistry:");
        Console.WriteLine("   1. Using Docker Compose:");
        Console.WriteLine("      docker compose up -d messagebroker schemaregistry");
        Console.WriteLine();
        Console.WriteLine("   2. Or manually:");
        Console.WriteLine("      cd MessageBroker/src && dotnet run");
        Console.WriteLine("      cd SchemaRegistry/src && dotnet run");
        Console.WriteLine();
        Console.WriteLine("   Then run the performance tests again.");
        Environment.Exit(1);
    }
    
    tcpClient.Close();
    Console.WriteLine("✅ MessageBroker is available");
}
catch (Exception ex)
{
    Console.WriteLine($"❌ ERROR: Cannot connect to MessageBroker at {brokerHost}:{brokerPort}");
    Console.WriteLine($"   Error: {ex.Message}");
    Console.WriteLine();
    Console.WriteLine("📋 Please ensure MessageBroker is running before starting performance tests.");
    Environment.Exit(1);
}
Console.WriteLine();

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

// Create scenarios
ScenarioProps publisherPerformanceScenario =
    PublisherPerformanceScenario.Create(publisherFactory, publisherOptions);

ScenarioProps endToEndScenario =
    EndToEndPerformanceScenario.Create(
        publisherFactory,
        publisherOptions,
        subscriberFactory,
        subscriberOptions);

ScenarioProps publisherLatencyScenario =
    PublisherLatencyScenario.Create(publisherFactory, publisherOptions);

// Run tests
NBomberRunner
    .RegisterScenarios(
        publisherPerformanceScenario,
        endToEndScenario,
        publisherLatencyScenario
    )
    .WithReportFolder("reports")
    .Run();

// Cleanup
httpClientFactory.Dispose();
if (schemaRegistryClient is IDisposable disposable)
{
    disposable.Dispose();
}

Console.WriteLine("\n✅ Performance tests completed!");
Console.WriteLine("📊 Check reports folder for detailed results.");
