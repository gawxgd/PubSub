using System;
using NBomber.CSharp;
using PerformanceTests.Models;
using PerformanceTests.Scenarios;

namespace PerformanceTests;

public class Program
{
    public static void Main(string[] args)
    {
        var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "127.0.0.1:9092";

        var scenarios = KafkaMultiTopicLongRunScenario.Create(
            bootstrapServers: bootstrap,
            numTopics: 10,
            producersPerTopic: 10,
            consumersPerTopic: 5,   
            totalRate: 5000,       
            durationSeconds: 60 * 60 * 2,
            drainSeconds: 10,
            maxE2ESamplesPerTopic: 200_000,
            deleteTopicsOnClean: false
        );

        KafkaMultiTopicLongRunScenario.StartSubscriberThroughputCsv("./reports/sub_throughput_1s.csv");

        NBomberRunner
            .RegisterScenarios(scenarios)
            .WithReportFolder("./reports")
            .WithTestName("Kafka_E2E_MultiTopic")
            .Run();

        KafkaMultiTopicLongRunScenario.SaveGlobalE2EToFile();
        KafkaMultiTopicLongRunScenario.StopSubscriberThroughputCsvAsync().GetAwaiter().GetResult();
    }
}
