using System.Runtime.CompilerServices;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;

namespace LoggerLib.Outbound.Adapter;

public class SignalRAutoLogger(
    Type type,
    ILogger logger,
    LogSource logSource,
    string timestampFormat = "yyyy-MM-dd HH:mm:ss.fff")
    : IAutoLogger
{
    private readonly string _className = type.Name;

    public void LogInfo(string message, 
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "")
    {
        var formattedMessage = FormatMessage(message, filePath, lineNumber, methodName);
        logger.LogInfo(logSource, formattedMessage);
    }

    public void LogError(string message, Exception? ex = null,
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "")
    {
        var formattedMessage = FormatMessage(message, filePath, lineNumber, methodName, ex);
        logger.LogError(logSource, formattedMessage);
    }

    public void LogWarning(string message, Exception? ex = null,
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "")
    {
        var formattedMessage = FormatMessage(message, filePath, lineNumber, methodName, ex);
        logger.LogWarning(logSource, formattedMessage);
    }

    public void LogDebug(string message, Exception? ex = null,
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "")
    {
        var formattedMessage = FormatMessage(message, filePath, lineNumber, methodName, ex);
        logger.LogDebug(logSource, formattedMessage);
    }

    public void LogTrace(string message, Exception? ex = null,
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "")
    {
        var formattedMessage = FormatMessage(message, filePath, lineNumber, methodName, ex);
        logger.LogTrace(logSource, formattedMessage);
    }

    private string FormatMessage(string message, string filePath, int lineNumber, string methodName,
        Exception? ex = null)
    {
        var fileName = Path.GetFileName(filePath);
        var timestamp = DateTime.UtcNow.ToString(timestampFormat);
        var context = $"{_className}.{methodName} ({fileName}:{lineNumber})";

        if (ex != null)
        {
            return $"[{timestamp}] {context} - {message} | Exception: {ex.Message}\nStackTrace: {ex.StackTrace}";
        }

        return $"[{timestamp}] {context} - {message}";
    }
}
