namespace MessageBroker.Domain.Entities.CommitLog;

public sealed record LogSegment(
    string LogPath,
    string IndexFilePath,
    string TimeIndexFilePath,
    ulong BaseOffset,
    ulong NextOffset
);
