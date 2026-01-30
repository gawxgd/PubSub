using System.Text.Json;
using BddE2eTests.Configuration.Options;

namespace BddE2eTests.Configuration;

public static class TestOptionsLoader
{
    private const string TestConfigFileName = "config.test.json";

    private static TestOptions? _cachedConfig;

    public static TestOptions Load()
    {
        if (_cachedConfig != null)
        {
            return _cachedConfig;
        }

        var configPath = Path.Combine(AppContext.BaseDirectory, TestConfigFileName);
        var json = File.ReadAllText(configPath);

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        _cachedConfig = JsonSerializer.Deserialize<TestOptions>(json, options)
                        ?? throw new InvalidOperationException($"Failed to deserialize {TestConfigFileName}");

        return _cachedConfig;
    }
}
