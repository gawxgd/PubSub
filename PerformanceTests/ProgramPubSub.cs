using System;
using System.Linq;
using System.Threading;
using LoggerLib.Outbound.Adapter;
using Microsoft.Extensions.DependencyInjection;
using NBomber.CSharp;
using PerformanceTests.Models;
using PerformanceTests.Scenarios;
using Publisher.Configuration;
using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Outbound.SchemaRegistryClient;
using Subscriber.Configuration;

namespace PerformanceTests;

public class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("╔════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║  E2E Multi-Topic Test - 10 topics × 5000 msg/s             ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════╝\n");

        var logger = new ConsoleLogger();
        AutoLoggerFactory.Initialize(logger);

        //ThreadPool.GetMinThreads(out int minWorker, out int minIOC);
        //Console.WriteLine($"Current ThreadPool: minWorker={minWorker}, minIOC={minIOC}");

        //ThreadPool.SetMinThreads(500, 500);

        var brokerHost = Environment.GetEnvironmentVariable("BROKER_HOST") ?? "127.0.0.1";
        var brokerPort = int.Parse(Environment.GetEnvironmentVariable("BROKER_PORT") ?? "9096");

        var brokerSubscriberPort = int.Parse(Environment.GetEnvironmentVariable("BROKER_SUBSCRIBER_PORT") ?? "9098");

        var schemaRegistryUrl = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://127.0.0.1:8081";
        var schemaRegistryUri = new Uri(schemaRegistryUrl);

        var schemaRegistryOptions = new SchemaRegistryClientOptions(schemaRegistryUri, TimeSpan.FromSeconds(10));

        var services = new ServiceCollection();

        services.AddHttpClient("SchemaRegistry", c =>
        {
            c.BaseAddress = schemaRegistryOptions.BaseAddress;
            c.Timeout = schemaRegistryOptions.Timeout;
        });

        using var sp = services.BuildServiceProvider();
        var httpClientFactory = sp.GetRequiredService<IHttpClientFactory>();
        var schemaRegistryClientFactory = new SchemaRegistryClientFactory(httpClientFactory, schemaRegistryOptions);

        var publisherFactory = new PublisherFactory<TestMessage>(schemaRegistryClientFactory);

        var schemaRegistryClient = schemaRegistryClientFactory.Create();
        var subscriberFactory = new SubscriberFactory<TestMessage>(schemaRegistryClient);

        Console.WriteLine("Config:");
        Console.WriteLine($"  Publisher broker:   messageBroker://{brokerHost}:{brokerPort}");
        Console.WriteLine($"  Subscriber broker:  messageBroker://{brokerHost}:{brokerSubscriberPort}");
        Console.WriteLine($"  Schema registry:    {schemaRegistryUri}");
        Console.WriteLine($"  Total throughput:   5000 msg/s\n");

        Console.WriteLine("Creating scenarios...");

        var scenarios = MultiTopicLongRunScenario.Create(
            publisherFactory: publisherFactory,
            subscriberFactory: subscriberFactory,
            brokerHost: brokerHost,
            brokerPort: brokerPort,
            brokerSubscriberPort: brokerSubscriberPort,
            schemaRegistryBaseAddress: schemaRegistryOptions.BaseAddress,
            schemaRegistryTimeout: schemaRegistryOptions.Timeout,
            maxPublisherQueueSize: 50_000u,
            numTopics: 1,
            publishersPerTopic: 1,
            subscribersPerTopic: 1,
            totalRate: 5000,
            durationSeconds: 60* 60 * 2,
            batchMaxBytesRaw: 65_536,
            batchMaxDelayRaw: TimeSpan.FromMilliseconds(50)
        );

        Console.WriteLine($"✓ {scenarios.Length} scenarios\n");
        Console.WriteLine("Starting test...\n");

        MultiTopicLongRunScenario.StartSubscriberThroughputCsv("./reports/sub_throughput_1s.csv");

        NBomberRunner
            .RegisterScenarios(scenarios)
            .WithReportFolder("./reports")
            .WithTestName("E2E_MultiTopic")
            .Run();

        MultiTopicLongRunScenario.SaveGlobalE2EToFile();
        MultiTopicLongRunScenario.StopSubscriberThroughputCsvAsync().GetAwaiter().GetResult();

        Console.WriteLine("\n✓ DONE!\n");
    }
}
