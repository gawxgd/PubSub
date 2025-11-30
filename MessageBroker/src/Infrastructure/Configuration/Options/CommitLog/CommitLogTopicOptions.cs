namespace MessageBroker.Infrastructure.Configuration.Options.CommitLog;

public sealed class CommitLogTopicOptions
{
    public string Name { get; set; } = "default";
    public ulong BaseOffset { get; set; } = 0;
    public uint FlushIntervalMs { get; set; } = 100;
    public string? Directory { get; set; }
}