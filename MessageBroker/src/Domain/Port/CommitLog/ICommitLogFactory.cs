namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogFactory
{
    ICommitLogAppender GetAppender(string topic);
    ICommitLogReader GetReader(string topic);
}
