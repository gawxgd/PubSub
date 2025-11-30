namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogFactoryM
{
    ICommitLogAppender GetAppender(string topic);
    ICommitLogReaderM GetReader(string topic);
}

public interface ICommitLogFactory
{
    ICommitLogAppender GetAppender(string topic);
    ICommitLogReader  GetReader(string topic, ulong offset); 
}