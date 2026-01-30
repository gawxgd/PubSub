namespace MessageBroker.Infrastructure.Configuration.Options.CommitLog;

public sealed class CommitLogOptions
{

    public string Directory { get; set; } = "logs";

    public ulong MaxSegmentBytes { get; set; } = 128 * 1024 * 1024;

    public uint IndexIntervalBytes { get; set; } = 4096;

    public uint FileBufferSize { get; set; } = 64 * 1024;

    public uint TimeIndexIntervalMs { get; set; } = 4096;

    public uint FlushIntervalMs { get; set; } = 100;
    public uint ReaderLogBufferSize { get; set; } = 64 * 1024;
    public uint ReaderIndexBufferSize { get; set; } = 8 * 1024;
}
