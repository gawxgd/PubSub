using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Reqnroll;
using Subscriber.Configuration;
using Subscriber.Outbound.Adapter;

namespace BddE2eTests.Steps.Subscriber.When;

[Binding]
public class SubscriberRestartWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [When(@"subscriber C restarts at offset (\d+)")]
    public async Task WhenSubscriberCRestartsAtOffset(ulong initialOffset)
    {
        await TestContext.Progress.WriteLineAsync($"[When Step] Restarting subscriber C at offset {initialOffset}...");

        await DisposeOldSubscriberSafelyAsync();

        await RestartSubscriberAtOffsetAsync(initialOffset);
    }

    private async Task DisposeOldSubscriberSafelyAsync()
    {
        if (_context.TryGetSubscriber(out var oldSubscriber) && oldSubscriber is IAsyncDisposable oldDisposable)
        {
            try
            {
                await TestContext.Progress.WriteLineAsync("[When Step] Disposing old subscriber...");
                await oldDisposable.DisposeAsync();
            }
            catch (ObjectDisposedException)
            {
                await TestContext.Progress.WriteLineAsync("[When Step] Old subscriber already disposed, skipping...");
            }
        }
    }

    private async Task RestartSubscriberAtOffsetAsync(ulong initialOffset)
    {
        await TestContext.Progress.WriteLineAsync($"[When Step] Restarting with initial offset: {initialOffset}");

        var builder = _context.GetOrCreateSubscriberOptionsBuilder();
        var schemaRegistryBuilder = _context.GetOrCreateSchemaRegistryClientBuilder();

        var topic = _context.Topic;
        await TestContext.Progress.WriteLineAsync($"[When Step] Using topic from context: {topic}");
        
        var subscriberOptions = builder.WithTopic(topic).Build();
        var schemaRegistryClient = schemaRegistryBuilder.Build();

        var receivedMessages = System.Threading.Channels.Channel.CreateUnbounded<TestEvent>();

        var subscriberFactory = new SubscriberFactory<TestEvent>(schemaRegistryClient);
        var newSubscriber = subscriberFactory.CreateSubscriber(subscriberOptions,
            async (message) => { await receivedMessages.Writer.WriteAsync(message); });

        if (newSubscriber is TcpSubscriber<TestEvent> tcpSubscriber)
        {
            await tcpSubscriber.StartConnectionAsync(initialOffset);
            await TestContext.Progress.WriteLineAsync(
                $"[When Step] Started connection with initial offset: {initialOffset}");
        }
        else
        {
            await newSubscriber.StartConnectionAsync();
        }

        _ = Task.Run(async () => await newSubscriber.StartMessageProcessingAsync());

        _context.Subscriber = newSubscriber;
        _context.ReceivedMessages = receivedMessages;

        await TestContext.Progress.WriteLineAsync("[When Step] Subscriber C restarted!");
    }
}