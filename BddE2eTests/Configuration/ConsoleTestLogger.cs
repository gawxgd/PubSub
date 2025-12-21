using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using NUnit.Framework;

namespace BddE2eTests.Configuration;

public class ConsoleTestLogger : ILogger
{
    public void LogInfo(LogSource source, string message)
    {
        TestContext.Progress.WriteLine($"[INFO] [{source}] {message}");
    }

    public void LogError(LogSource source, string message)
    {
        TestContext.Progress.WriteLine($"[ERROR] [{source}] {message}");
    }

    public void LogDebug(LogSource source, string message)
    {
        TestContext.Progress.WriteLine($"[DEBUG] [{source}] {message}");
    }

    public void LogWarning(LogSource source, string message)
    {
        TestContext.Progress.WriteLine($"[WARN] [{source}] {message}");
    }

    public void LogTrace(LogSource source, string message)
    {
        TestContext.Progress.WriteLine($"[TRACE] [{source}] {message}");
    }
}
