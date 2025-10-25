using System.Runtime.CompilerServices;

namespace LoggerLib.Domain.Port;

public interface IAutoLogger
{
    void LogInfo(string message, [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "");

    void LogError(string message, Exception? ex = null,
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "");

    void LogWarning(string message, Exception? ex = null,
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "");

    void LogDebug(string message, Exception? ex = null,
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "");

    void LogTrace(string message, Exception? ex = null,
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0,
        [CallerMemberName] string methodName = "");
}