namespace MessageBroker.Domain.Entities.CommitLog;

public record LogSegmentRecord(
    string FilePath,
    ulong BaseOffset,
    ulong NextOffset,
    bool IsClosed
);