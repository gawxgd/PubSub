using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Reqnroll;
using Subscriber.Configuration;
using Subscriber.Outbound.Adapter;
using System.Reflection;

namespace BddE2eTests.Steps.Subscriber.When;

[Binding]
public class SubscriberRestartWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);
    private System.Threading.Channels.Channel<ITestEvent>? _restartReceivedMessages;

    [When(@"subscriber C restarts at offset (\d+)")]
    public async Task WhenSubscriberCRestartsAtOffset(ulong initialOffset)
    {
        await TestContext.Progress.WriteLineAsync($"[When Step] Restarting subscriber C at offset {initialOffset}...");

        await DisposeOldSubscriberSafelyAsync();

        await RestartSubscriberAtOffsetAsync(initialOffset);
    }

    private async Task DisposeOldSubscriberSafelyAsync()
    {
        if (_context.TryGetSubscriber(out var oldSubscriber))
        {
            try
            {
                await TestContext.Progress.WriteLineAsync("[When Step] Disposing old subscriber...");
                await oldSubscriber!.DisposeAsync();
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

        var receivedMessages = System.Threading.Channels.Channel.CreateUnbounded<ITestEvent>();
        _restartReceivedMessages = receivedMessages;
        var messageType = _context.Subscriber.MessageType;

        var subscriberFactoryType = typeof(SubscriberFactory<>).MakeGenericType(messageType);
        var subscriberFactory = Activator.CreateInstance(subscriberFactoryType, schemaRegistryClient)!;
        var handler = CreateHandlerDelegate(messageType);
        var newSubscriber = ((dynamic)subscriberFactory).CreateSubscriber(subscriberOptions, (dynamic)handler);

        var tcpSubscriberType = typeof(TcpSubscriber<>).MakeGenericType(messageType);
        if (tcpSubscriberType.IsInstanceOfType(newSubscriber))
        {
            await ((dynamic)newSubscriber).StartConnectionAsync((ulong?)initialOffset);
            await TestContext.Progress.WriteLineAsync(
                $"[When Step] Started connection with initial offset: {initialOffset}");
        }
        else
        {
            await ((dynamic)newSubscriber).StartConnectionAsync((ulong?)null);
        }

        _ = Task.Run(async () => await ((dynamic)newSubscriber).StartMessageProcessingAsync());

        _context.Subscriber = new SubscriberHandle(newSubscriber, messageType, receivedMessages);
        _context.ReceivedMessages = receivedMessages;

        await TestContext.Progress.WriteLineAsync("[When Step] Subscriber C restarted!");
    }

    private Delegate CreateHandlerDelegate(Type messageType)
    {
        var method = GetType()
            .GetMethod(nameof(WriteReceivedAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(messageType);

        var delegateType = typeof(Func<,>).MakeGenericType(messageType, typeof(Task));
        return Delegate.CreateDelegate(delegateType, this, method);
    }

    private Task WriteReceivedAsync<TEvent>(TEvent message) where TEvent : ITestEvent
    {
        if (_restartReceivedMessages == null)
        {
            throw new InvalidOperationException("Restart received messages channel is not initialized");
        }

        return _restartReceivedMessages.Writer.WriteAsync(message).AsTask();
    }
}