using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Publisher.Configuration;
using Reqnroll;

namespace BddE2eTests.Steps.Publisher.When;

[Binding]
public class PublisherRestartWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [When(@"the publisher restarts")]
    public async Task WhenThePublisherRestarts()
    {
        await TestContext.Progress.WriteLineAsync("[When Step] Restarting publisher...");

        await DisposeOldPublisherSafelyAsync();

        await RestartPublisherAsync();
    }

    private async Task DisposeOldPublisherSafelyAsync()
    {
        if (_context.TryGetPublisher(out var oldPublisher) && oldPublisher is IAsyncDisposable oldDisposable)
        {
            try
            {
                await TestContext.Progress.WriteLineAsync("[When Step] Disposing old publisher...");
                await oldDisposable.DisposeAsync();
            }
            catch (ObjectDisposedException)
            {
                await TestContext.Progress.WriteLineAsync("[When Step] Old publisher already disposed, skipping...");
            }
        }
    }

    private async Task RestartPublisherAsync()
    {
        await TestContext.Progress.WriteLineAsync("[When Step] Restarting publisher...");

        var builder = _context.GetOrCreatePublisherOptionsBuilder();
        var schemaRegistryBuilder = _context.GetOrCreateSchemaRegistryClientBuilder();

        var publisherOptions = builder.Build();
        var schemaRegistryClientFactory = schemaRegistryBuilder.BuildFactory();

        var publisherFactory = new PublisherFactory<TestEvent>(schemaRegistryClientFactory);
        var newPublisher = publisherFactory.CreatePublisher(publisherOptions);

        await TestContext.Progress.WriteLineAsync("[When Step] Connecting to broker...");
        await newPublisher.CreateConnection();
        await TestContext.Progress.WriteLineAsync("[When Step] Publisher connected!");

        _context.Publisher = newPublisher;

        await TestContext.Progress.WriteLineAsync("[When Step] Publisher restarted!");
    }
}

