namespace MessageBroker.Infrastructure.Configuration.Options.CommitLog;

public sealed class CommitLogOptions
{
    /// <summary>
    /// Directory where commit log segments are stored.
    /// </summary>
    public string Directory { get; set; } = "logs";

    /// <summary>
    /// Maximum size of a single log segment in bytes before rolling.
    /// </summary>
    public ulong MaxSegmentBytes { get; set; } = 128 * 1024 * 1024; // 128 MB default

    /// <summary>
    /// Number of bytes between index entries.
    /// </summary>
    public uint IndexIntervalBytes { get; set; } = 4096; // 4 KB default

    /// <summary>
    /// File buffer size for FileStreams in bytes.
    /// </summary>
    public uint FileBufferSize { get; set; } = 64 * 1024; // 64 KB default

    /// <summary>
    /// Optional: Time index interval in Ms (if you want time-based indexing).
    /// </summary>
    public uint TimeIndexIntervalMs { get; set; } = 4096;

    public uint FlushIntervalMs { get; set; } = 100;
    public uint ReaderLogBufferSize { get; set; } = 64 * 1024;
    public uint ReaderIndexBufferSize { get; set; } = 8 * 1024;
}