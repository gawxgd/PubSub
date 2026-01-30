using LoggerLib.Domain.Enums;

namespace LoggerLib.Domain.Port;

public interface ILogger
{
    public void LogInfo(LogSource source, string message);
    public void LogError(LogSource source, string message);
    public void LogDebug(LogSource source, string message);
    public void LogWarning(LogSource source, string message);
    public void LogTrace(LogSource source, string message);
}
