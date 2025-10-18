# PubSub Demo - Quick Start Guide

Get the demo running in 2 minutes!

## Prerequisites

- .NET 9.0 SDK installed
- Terminal/Command Prompt

## Quick Start

### Step 1: Start the MessageBroker

Open a terminal and run:

```bash
cd MessageBroker
dotnet run
```

You should see:
```
Created socket with options: ServerOptions: Port=9096, Address=127.0.0.1...
Application started. Press Ctrl+C to shut down.
```

### Step 2: Start the Demo (in a new terminal)

```bash
cd PubSubDemo
dotnet run
```

You should see:
```
╔════════════════════════════════════════════╗
║   PubSub Demo - Message Publisher         ║
║   Demonstrates TcpPublisher Usage          ║
╚════════════════════════════════════════════╝

📡 Broker Configuration:
   Host: 127.0.0.1
   Port: 9096
   Queue Size: 10000

Connected to message broker successfully!
🚀 Publishing messages... Press Ctrl+C to stop.

[19:30:45] Sent: 5 | Failed: 0 | Last: Event
```

### Step 3: Stop the Demo

Press `Ctrl+C` to stop:

```
^C
🛑 Shutdown signal received...
Stopping Message Publisher Service...
Service stopped. Messages sent: 42, Failed: 0
✅ Graceful shutdown complete.
```

## Testing Reconnection

### 1. Start Demo with Broker Running
```bash
# Terminal 1
cd MessageBroker && dotnet run

# Terminal 2
cd PubSubDemo && dotnet run
```

### 2. Stop the Broker
Press `Ctrl+C` in Terminal 1. Watch the demo continue trying to reconnect.

### 3. Restart the Broker
```bash
cd MessageBroker && dotnet run
```

Watch the demo automatically reconnect and resume publishing! ✅

## Configuration Examples

### Faster Publishing (100ms interval)
```bash
dotnet run --Demo:MessageInterval=100
```

### Larger Batches
```bash
dotnet run --Demo:BatchSize=50
```

### Custom Broker Address
```bash
dotnet run --Broker:Host=192.168.1.100 --Broker:Port=9096
```

### Environment Variables
```bash
export PUBSUB_Demo__MessageInterval=500
export PUBSUB_Broker__Host=localhost
dotnet run
```

## What You're Seeing

The demo continuously:
1. **Publishes** JSON messages with random types (Info, Warning, Error, etc.)
2. **Tracks** success/failure counts in real-time
3. **Sends Batches** every 50 messages (10 messages at once)
4. **Handles Errors** gracefully with automatic retry
5. **Reconnects** automatically if broker restarts

## Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Modify `MessagePublisherService.cs` to customize message content
- Adjust `appsettings.json` for different behaviors
- Check MessageBroker logs to see received messages

## Troubleshooting

**Problem**: `Connection refused`
**Solution**: Make sure MessageBroker is running on port 9096

**Problem**: `Address already in use`
**Solution**: Stop any other process using port 9096, or change the port in both apps

**Problem**: Demo stops sending
**Solution**: Check MessageBroker is still running and responding

## Have Fun! 🎉

This demo shows real-world usage of the `TcpPublisher` component. Try breaking things:
- Stop the broker mid-publish
- Send thousands of messages
- Change configurations on the fly

The publisher is designed to handle all of it gracefully!

