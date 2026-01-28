using BddE2eTests.Configuration;
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
        if (_context.TryGetPublisher(out var oldPublisher))
        {
            try
            {
                await TestContext.Progress.WriteLineAsync("[When Step] Disposing old publisher...");
                await oldPublisher!.DisposeAsync();
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

        var publisherOptions = builder.WithTopic(_context.Topic!).Build();
        await TestContext.Progress.WriteLineAsync($"[When Step] Using topic from context: {_context.Topic}");
        
        var schemaRegistryClientFactory = schemaRegistryBuilder.BuildFactory();

        var messageType = _context.Publisher.MessageType;
        var publisherFactoryType = typeof(PublisherFactory<>).MakeGenericType(messageType);
        var publisherFactory = Activator.CreateInstance(publisherFactoryType, schemaRegistryClientFactory)!;
        var newPublisher = ((dynamic)publisherFactory).CreatePublisher(publisherOptions);

        await TestContext.Progress.WriteLineAsync("[When Step] Connecting to broker...");
        await ((dynamic)newPublisher).CreateConnection();
        await TestContext.Progress.WriteLineAsync("[When Step] Publisher connected!");

        _context.Publisher = new PublisherHandle(newPublisher, messageType);

        await TestContext.Progress.WriteLineAsync("[When Step] Publisher restarted!");
    }
}

