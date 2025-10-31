using MessageBroker.Domain.Entities.CommitLog.Index;

namespace MessageBroker.Domain.Port.CommitLog.Index.Writer;

public interface ITimeIndexWriter
{
    Task WriteToAsync(TimeIndexEntry entry, Stream stream);
}