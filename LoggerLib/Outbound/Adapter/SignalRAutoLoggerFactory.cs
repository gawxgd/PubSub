using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;

namespace LoggerLib.Outbound.Adapter;

public static class AutoLoggerFactory
{
    private static ILogger? _logger;
    private static string _timestampFormat = "yyyy-MM-dd HH:mm:ss.fff";

    public static void Initialize(ILogger logger, string timestampFormat = "yyyy-MM-dd HH:mm:ss.fff")
    {
        _logger = logger;
        _timestampFormat = timestampFormat;
    }

    public static IAutoLogger CreateLogger<T>(LogSource logSource)
    {
        EnsureInitialized();
        return new SignalRAutoLogger(typeof(T), _logger!, logSource, _timestampFormat);
    }

    public static IAutoLogger CreateLogger(Type type, LogSource logSource)
    {
        EnsureInitialized();
        return new SignalRAutoLogger(type, _logger!, logSource, _timestampFormat);
    }

    private static void EnsureInitialized()
    {
        if (_logger == null)
        {
            throw new InvalidOperationException(
                "AutoLoggerFactory must be initialized before use. Call AutoLoggerFactory.Initialize() at startup.");
        }
    }
}
