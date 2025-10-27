namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogFactory
{
    ICommitLogAppender Get(string topic);
}