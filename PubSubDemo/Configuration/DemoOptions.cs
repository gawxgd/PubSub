namespace PubSubDemo.Configuration;

public sealed class DemoOptions
{

    public int MessageInterval { get; set; } = 1000;

    public string MessagePrefix { get; set; } = "Demo";

    public int BatchSize { get; set; } = 10;
    
    public string Topic { get; set; } = "DemoTopic";

    public string[] Topics { get; set; } = new[] { "default", "metrics", "audit" };
    
    public int BatchMaxBytes { get; set; } = 65536;
    
    public TimeSpan BatchMaxDelay { get; set; } = TimeSpan.FromSeconds(10);
    
}

