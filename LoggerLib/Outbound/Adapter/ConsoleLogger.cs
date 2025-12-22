using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;

namespace LoggerLib.Outbound.Adapter;

/// <summary>
///     A simple console logger implementation for console applications.
/// </summary>
public class ConsoleLogger : ILogger
{
    public void LogInfo(LogSource source, string message)
    {
        Console.WriteLine($"[INFO] [{source}] {message}");
    }

    public void LogError(LogSource source, string message)
    {
        Console.Error.WriteLine($"[ERROR] [{source}] {message}");
    }

    public void LogDebug(LogSource source, string message)
    {
        Console.WriteLine($"[DEBUG] [{source}] {message}");
    }

    public void LogWarning(LogSource source, string message)
    {
        Console.WriteLine($"[WARN] [{source}] {message}");
    }

    public void LogTrace(LogSource source, string message)
    {
        Console.WriteLine($"[TRACE] [{source}] {message}");
    }
}

