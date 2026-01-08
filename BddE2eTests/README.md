# BDD E2E Tests

This project contains end-to-end tests for the PubSub system using BDD (Behavior-Driven Development) with Reqnroll/NUnit.

## Prerequisites

- .NET 9.0 SDK
- Ports 9096, 9098, and 8081 must be available

## Running Tests

### Run all E2E tests

```bash
cd BddE2eTests
dotnet test
```

### Run a specific test

```bash
dotnet test --filter "TestName"
```

For example:

```bash
dotnet test --filter "PublisherContinuesSendingMessagesDuringBrokerRestart"
```

### Run with verbose output

```bash
dotnet test --logger "console;verbosity=detailed"
```

## Troubleshooting

### Tests hang indefinitely

If tests hang at "Creating broker host..." or similar, it's likely due to **zombie processes from a previous test run** holding onto the required ports.

#### Solution: Kill lingering test processes

**macOS/Linux:**

```bash
pkill -f 'dotnet.*BddE2eTests'
```

**Windows (PowerShell):**

```powershell
Get-Process dotnet | Where-Object { $_.CommandLine -like '*BddE2eTests*' } | Stop-Process -Force
```

#### Verify ports are free

**macOS/Linux:**

```bash
lsof -i :9096 -i :9098 -i :8081
```

**Windows:**

```cmd
netstat -ano | findstr "9096 9098 8081"
```

### Timeout errors

Tests have a default timeout of 60 seconds. If you see timeout errors:

1. Check if the broker/schema registry started correctly
2. Verify no port conflicts exist
3. Run the cleanup commands above

## Test Configuration

Tests use `config.test.json` for configuration:

| Port | Service |
|------|---------|
| 9096 | Message Broker (Publisher) |
| 9098 | Message Broker (Subscriber) |
| 8081 | Schema Registry |

## Test Structure

- **Features/** - Gherkin feature files defining test scenarios
- **Steps/** - Step definitions implementing the Gherkin steps
- **Configuration/** - Test configuration and context helpers
- **TestBase.cs** - Common test setup/teardown logic

## Pre-run Cleanup Script

For CI/CD or before running tests locally, you can use this cleanup script:

```bash
#!/bin/bash
# cleanup-before-tests.sh

echo "Cleaning up any lingering test processes..."
pkill -f 'dotnet.*BddE2eTests' 2>/dev/null || true

echo "Waiting for ports to be released..."
sleep 2

echo "Verifying ports are free..."
for port in 9096 9098 8081; do
    if lsof -i :$port > /dev/null 2>&1; then
        echo "WARNING: Port $port is still in use!"
        lsof -i :$port
    else
        echo "Port $port is free"
    fi
done

echo "Ready to run tests!"
```

