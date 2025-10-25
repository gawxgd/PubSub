namespace LoggerLib.Domain.Model;

public sealed record AutoLoggerContext(
    string Timestamp,
    string ContextString,
    string ClassName,
    string MethodName,
    string FileName,
    int LineNumber);