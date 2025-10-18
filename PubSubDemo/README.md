# PubSub Demo Service

A demonstration service that shows how to use the `TcpPublisher` component to publish messages to the MessageBroker.

## Features

- ✅ **Automatic Connection Management** - Connects to broker with automatic retry
- ✅ **Message Publishing** - Continuously publishes JSON messages
- ✅ **Batch Publishing** - Periodically sends batches of messages
- ✅ **Graceful Shutdown** - Handles Ctrl+C for clean shutdown
- ✅ **Configuration** - JSON-based configuration with environment variable overrides
- ✅ **Error Handling** - Robust error handling with retry logic
- ✅ **Real-time Stats** - Shows sent/failed message counts

## Architecture

```
PubSubDemo
├── Program.cs                          # Entry point and setup
├── Configuration/
│   ├── BrokerOptions.cs               # Broker connection settings
│   └── DemoOptions.cs                 # Demo behavior settings
├── Services/
│   └── MessagePublisherService.cs     # Main publishing service
└── appsettings.json                   # Configuration file
```

## Configuration

### appsettings.json

```json
{
  "Broker": {
    "Host": "127.0.0.1",
    "Port": 9096,
    "MaxQueueSize": 10000
  },
  "Demo": {
    "MessageInterval": 1000,
    "MessagePrefix": "Demo",
    "BatchSize": 10
  }
}
```

### Environment Variables

Prefix environment variables with `PUBSUB_`:

```bash
export PUBSUB_Broker__Host=localhost
export PUBSUB_Broker__Port=9096
export PUBSUB_Demo__MessageInterval=500
```

### Command Line Arguments

```bash
dotnet run --Broker:Host=localhost --Broker:Port=9096 --Demo:MessageInterval=2000
```

## Usage

### 1. Start the MessageBroker

First, ensure the MessageBroker is running:

```bash
cd ../MessageBroker
dotnet run
```

### 2. Run the Demo

In a separate terminal:

```bash
cd PubSubDemo
dotnet run
```

### 3. Watch the Output

You'll see real-time statistics:

```
╔════════════════════════════════════════════╗
║   PubSub Demo - Message Publisher         ║
║   Demonstrates TcpPublisher Usage          ║
╚════════════════════════════════════════════╝

📡 Broker Configuration:
   Host: 127.0.0.1
   Port: 9096
   Queue Size: 10000

⚙️  Demo Configuration:
   Message Interval: 1000ms
   Message Prefix: Demo
   Batch Size: 10

Starting Message Publisher Service...
Connected to message broker successfully!
🚀 Publishing messages... Press Ctrl+C to stop.

[19:30:45] Sent: 42 | Failed: 0 | Last: Event
```

### 4. Stop the Service

Press `Ctrl+C` to gracefully shutdown:

```
^C
🛑 Shutdown signal received...

Stopping Message Publisher Service...
Service stopped. Messages sent: 42, Failed: 0
✅ Graceful shutdown complete.

👋 PubSub Demo finished.
```

## Message Format

The demo publishes JSON messages in the following format:

```json
{
  "Id": 42,
  "Timestamp": "2025-10-18T19:30:45.123Z",
  "Content": "Demo message #42",
  "Source": "PubSubDemo",
  "MessageType": "Event"
}
```

## Key Components

### TcpPublisher Usage

```csharp
// Create publisher with configuration
await using var publisher = new TcpPublisher(
    host: "127.0.0.1",
    port: 9096,
    maxQueueSize: 10000);

// Connect to broker (with automatic retry)
await publisher.ConnectAsync(cancellationToken);

// Publish messages
var message = Encoding.UTF8.GetBytes("Hello, World!");
await publisher.PublishAsync(message, cancellationToken);

// Automatic disposal and cleanup
```

### Error Handling

The demo includes comprehensive error handling:

- **Connection Errors**: Automatic retry with exponential backoff
- **Publishing Errors**: Logged and tracked, continues operation
- **Shutdown**: Graceful cleanup of all resources

### Batch Publishing

Every 50 messages, the demo sends a batch of 10 messages to demonstrate burst traffic:

```
📦 Sending batch of 10 messages...
✅ Batch complete!
```

## Testing Scenarios

### 1. Normal Operation
```bash
dotnet run
```

### 2. Fast Publishing
```bash
dotnet run --Demo:MessageInterval=100
```

### 3. Large Batches
```bash
dotnet run --Demo:BatchSize=100
```

### 4. Custom Message Prefix
```bash
dotnet run --Demo:MessagePrefix=MyApp
```

### 5. Broker Reconnection

Start demo, then:
1. Stop the MessageBroker
2. Watch demo handle disconnection
3. Restart MessageBroker
4. Watch demo automatically reconnect

## Performance

The demo is designed to:
- Handle high message throughput (configurable interval)
- Maintain stable memory usage (bounded channel queue)
- Automatically recover from broker restarts
- Track success/failure metrics

## Extending the Demo

### Add Custom Message Types

Edit `MessagePublisherService.cs`:

```csharp
public sealed class CustomMessage
{
    public string CustomField { get; set; }
    // Add your fields
}
```

### Add Multiple Publishers

Create multiple `MessagePublisherService` instances with different configurations:

```csharp
var publisher1 = new TcpPublisher(host, port, queueSize);
var service1 = new MessagePublisherService(publisher1, options1);

var publisher2 = new TcpPublisher(host, port, queueSize);
var service2 = new MessagePublisherService(publisher2, options2);
```

### Add Metrics

Integrate with metrics libraries like Prometheus or Application Insights:

```csharp
_metricsCollector.RecordMessageSent();
_metricsCollector.RecordPublishLatency(latency);
```

## Troubleshooting

### Connection Refused

**Problem**: `SocketException: Connection refused`

**Solution**: Ensure MessageBroker is running on the specified host:port

### Queue Full

**Problem**: Messages backing up in queue

**Solution**: 
- Increase `MaxQueueSize` in configuration
- Check broker is processing messages
- Reduce `MessageInterval` to send slower

### High Memory Usage

**Problem**: Memory grows continuously

**Solution**:
- Verify `MaxQueueSize` is appropriate
- Check for broker connectivity issues
- Monitor message processing rate

## Production Considerations

When adapting this demo for production:

1. **Logging**: Add structured logging (Serilog, NLog)
2. **Metrics**: Add telemetry and monitoring
3. **Health Checks**: Implement health check endpoints
4. **Configuration**: Use proper secrets management
5. **Error Handling**: Implement dead letter queues
6. **Retry Policies**: Configure retry strategies
7. **Rate Limiting**: Add backpressure mechanisms
8. **Monitoring**: Add alerting for failures

## Related Projects

- **MessageBroker** - The TCP server that receives messages
- **Publisher** - The TcpPublisher library used by this demo
- **Subscriber** - Component for receiving messages (future)

## License

Part of the PubSub project.

