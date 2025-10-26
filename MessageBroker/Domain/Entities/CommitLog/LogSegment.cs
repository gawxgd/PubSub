namespace MessageBroker.Domain.Entities.CommitLog;

public sealed record LogSegment(
    string LogPath,
    string IndexFilePath,
    string TimeIndexFilePath,
    ulong BaseOffset,
    ulong NextOffset
    // Dictionary<long, long> Index, ToDo do we need it
    // bool IsClosed ToDo do we need it
);