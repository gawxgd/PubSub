using MessageBroker.Domain.Entities.CommitLog.Index;

namespace MessageBroker.Domain.Port.CommitLog.Index.Reader;

public interface IOffsetIndexReader
{
    OffsetIndexEntry ReadFrom(Stream stream);
}