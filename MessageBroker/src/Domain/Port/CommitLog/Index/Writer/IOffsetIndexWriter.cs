using MessageBroker.Domain.Entities.CommitLog.Index;

namespace MessageBroker.Domain.Port.CommitLog.Index.Writer;

public interface IOffsetIndexWriter
{
    Task WriteToAsync(OffsetIndexEntry entry, Stream stream);
}