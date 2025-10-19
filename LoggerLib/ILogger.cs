namespace LoggerLib;

public interface ILogger
{
    public void LogInfo(LogSource source, string message);
    public void LogError(LogSource source, string message);
    public void LogDebug(LogSource source, string message);

}