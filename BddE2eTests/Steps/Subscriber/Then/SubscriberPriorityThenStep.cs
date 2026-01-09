using System.Text.Json;
using System.Threading.Channels;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Reqnroll;
using Shared.Domain.Avro;
using System.Reflection;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class SubscriberPriorityThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 10;

    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"the subscriber receives message ""(.*)"" with priority null")]
    public async Task ThenTheSubscriberReceivesMessageWithNullPriority(string message)
    {
        var received = await ReceiveSingleAsync(_context.ReceivedMessages);
        Assert.That(received.Message, Is.EqualTo(message));
        var priority = TryGetPriority(received);
        Assert.That(priority, Is.Null);
    }

    [Then(@"the subscriber receives message ""(.*)"" with priority (\d+)")]
    public async Task ThenTheSubscriberReceivesMessageWithPriority(string message, int priority)
    {
        var received = await ReceiveSingleAsync(_context.ReceivedMessages);
        Assert.That(received.Message, Is.EqualTo(message));
        var actual = TryGetPriority(received);
        Assert.That(actual, Is.EqualTo(priority));
    }
    
    [Then(@"the subscriber receives message ""(.*)"" with default priority")]
    public async Task ThenTheSubscriberReceivesMessageWithDefaultPriority(string message)
    {
        var received = await ReceiveSingleAsync(_context.ReceivedMessages);
        Assert.That(received.Message, Is.EqualTo(message));

        var actual = TryGetPriority(received);
        Assert.That(actual, Is.Not.Null, "Expected message to have a Priority field");

        var defaultPriority = GetPriorityDefaultFromReaderSchema(_context.Subscriber.MessageType);
        Assert.That(actual, Is.EqualTo(defaultPriority));
    }

    private static async Task<ITestEvent> ReceiveSingleAsync(Channel<ITestEvent> receivedMessages)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));
        try
        {
            return await receivedMessages.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for a message");
            throw;
        }
    }

    private static int? TryGetPriority(ITestEvent received)
    {
        var prop = received.GetType().GetProperty("Priority");
        if (prop == null)
        {
            return null;
        }

        var value = prop.GetValue(received);
        return value switch
        {
            null => null,
            int i => i,
            _ => throw new InvalidOperationException(
                $"Priority property on type '{received.GetType().Name}' is not an int (value: {value})")
        };
    }

    private static int GetPriorityDefaultFromReaderSchema(Type readerType)
    {
        // We treat the subscriber's T as readersSchema and read the default from that schema.
        var schemaJson = GenerateSchemaJsonFor(readerType);
        using var doc = JsonDocument.Parse(schemaJson);

        if (!doc.RootElement.TryGetProperty("fields", out var fields) || fields.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("Schema JSON does not contain 'fields' array");
        }

        foreach (var field in fields.EnumerateArray())
        {
            if (field.ValueKind != JsonValueKind.Object) continue;
            if (!field.TryGetProperty("name", out var nameEl)) continue;
            if (!string.Equals(nameEl.GetString(), "Priority", StringComparison.Ordinal)) continue;

            if (!field.TryGetProperty("default", out var defEl))
            {
                throw new InvalidOperationException("Reader schema field 'Priority' has no 'default'");
            }

            if (defEl.ValueKind == JsonValueKind.Number && defEl.TryGetInt32(out var i))
            {
                return i;
            }

            throw new InvalidOperationException($"Unsupported default value kind for Priority: {defEl.ValueKind}");
        }

        throw new InvalidOperationException("Reader schema does not define field 'Priority'");
    }

    private static string GenerateSchemaJsonFor(Type t)
    {
        var method = typeof(AvroSchemaGenerator)
            .GetMethod(nameof(AvroSchemaGenerator.GenerateSchemaJson), BindingFlags.Public | BindingFlags.Static)!;

        var closed = method.MakeGenericMethod(t);
        return (string)closed.Invoke(null, null)!;
    }
}

