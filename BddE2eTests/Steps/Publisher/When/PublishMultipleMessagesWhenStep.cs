using System.Text;
using Reqnroll;
using BddE2eTests.Configuration;

namespace BddE2eTests.Steps.Publisher.When;

[Binding]
public class PublishMultipleMessagesWhenStep(ScenarioContext scenarioContext)
{
    private const string MessageColumn = "Message";
    
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [When(@"the publisher sends messages in order:")]
    public async Task WhenThePublisherSendsMessagesInOrder(Table table)
    {
        var publisher = _context.Publisher;
        var topic = _context.Topic;

        foreach (var row in table.Rows)
        {
            var message = row[MessageColumn];
            var messageBytes = Encoding.UTF8.GetBytes(message);
            
            await publisher.PublishAsync(messageBytes);
        }
    }
}

