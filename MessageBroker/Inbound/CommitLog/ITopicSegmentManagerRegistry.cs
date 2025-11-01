namespace MessageBroker.Inbound.CommitLog;

public interface ITopicSegmentManagerRegistry : IDisposable
{
    ITopicSegmentManager GetOrCreate(string topic, string directory, ulong baseOffset);
}


