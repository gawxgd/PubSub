# PubSub

A .NET publisher subscriber system.

---

## Table of contents

- [Running the demo](#running-the-demo)
- [Running tests](#running-tests)
- [Using the Publisher and Subscriber libraries](#using-the-publisher-and-subscriber-libraries)
- [Running with Docker (broker and full stack)](#running-with-docker-broker-and-full-stack)
- [Troubleshooting](#troubleshooting)

---

## Running the demo

The simplest way to run the demo is with Docker. From the repository root:

```bash
docker compose up -d
```

This starts the **MessageBroker**, **Schema Registry**, **PubSubDemo** (Publisher + Subscriber), and the **Frontend**. Open **http://localhost:3000** to see the statistics dashboard (messages published/consumed, connections, topics). No other setup is required.

**Prerequisites:** [Docker](https://docs.docker.com/get-docker/) and Docker Compose.

To stop: `docker compose down`.

---

## Running tests

### Unit, Integration, BDD E2E tests

From the repository root:

```bash
dotnet test
```

This runs all unit, integration and BDD E2E test projects in the solution:

- **MessageBroker.UnitTests** – broker unit tests  
- **MessageBroker.IntegrationTests** – broker integration tests (commit log, etc.)  
- **SchemaRegistry.Tests** – schema registry unit tests  
- **BddE2eTests** – BDD E2E scenarios (Reqnroll/NUnit)

To run a specific test project:

```bash
# MessageBroker unit tests
dotnet test MessageBroker/test/MessageBroker.UnitTests/MessageBroker.UnitTests.csproj

# MessageBroker integration tests
dotnet test MessageBroker/test/MessageBroker.IntegrationTests/MessageBroker.IntegrationTests.csproj

# SchemaRegistry tests
dotnet test SchemaRegistry/test/SchemaRegistry.Tests/SchemaRegistry.Tests.csproj

# BDD E2E tests
dotnet test BddE2eTests/BddE2eTests.csproj
```

### Performance tests

The **PerformanceTests** project uses NBomber to load-test the PubSub broker (and optionally Kafka). It uses the same Publisher/Subscriber libraries as the demo.

**Prerequisites:** MessageBroker and Schema Registry must be running (same as the demo).

The project is set up so that one of the `Program*.cs` files is the entry point. By default the solution expects a **PubSub** entry point. If your repo has **ProgramPubSub.cs** but no **Program.cs**, do one of the following:

- **Option A:** Copy `PerformanceTests/ProgramPubSub.cs` to `PerformanceTests/Program.cs`, or  
- **Option B:** In `PerformanceTests/PerformanceTests.csproj`, remove `ProgramPubSub.cs` from the `<Compile Remove="..."/>` list so it is compiled as the main program.

Then run:

```bash
# From repo root
dotnet run --project PerformanceTests
```

Optional environment variables:

- `BROKER_HOST` – broker host (default: `127.0.0.1`)
- `BROKER_PORT` – broker publisher port (default: `9096`)
- `BROKER_SUBSCRIBER_PORT` – broker subscriber port (default: `9098`)
- `SCHEMA_REGISTRY_URL` – schema registry URL (default: `http://127.0.0.1:8081`)

Reports are written under `PerformanceTests/reports/`.

To use the **Kafka** scenario instead, follow the comments in `PerformanceTests/PerformanceTests.csproj`: rename the current entry program (e.g. `Program.cs` → `ProgramPubSub.cs`) and `ProgramKafka.cs` → `Program.cs`, then run again. Kafka and related services must be running for that scenario.

---

## Using the Publisher and Subscriber libraries

You can use the **Publisher** and **Subscriber** libraries in your own .NET applications. They depend on:

- **MessageBroker** (TCP protocol and types)
- **Shared** (schema registry client, etc.)
- **LoggerLib** (logging)

Add project references from your app to `Publisher` and/or `Subscriber`; the rest are pulled in transitively.

### Example: .csproj references

```xml
<ItemGroup>
  <ProjectReference Include="path/to/Publisher/Publisher.csproj" />
  <ProjectReference Include="path/to/Subscriber/Subscriber.csproj" />
  <ProjectReference Include="path/to/LoggerLib/LoggerLib.csproj" />
  <ProjectReference Include="path/to/Shared/Shared.csproj" />
</ItemGroup>
```

If your project already references **Publisher** or **Subscriber** from within this repo, you usually do **not** need to reference MessageBroker explicitly (it is referenced by Publisher and Subscriber). You do need **Shared** and **LoggerLib** if you use the same configuration and logging patterns as the demo.

### Publisher

1. Define a message type (e.g. a class) that will be serialized with Avro. Register its schema in the Schema Registry (see SchemaRegistry docs).
2. Create a schema registry client factory and a `PublisherFactory<T>` for your message type.
3. Build `PublisherOptions` (broker URI, schema registry URI, topic, batch and retry settings).
4. Create a publisher with `publisherFactory.CreatePublisher(options)` and connect/publish.

Minimal pattern:

```csharp
using Publisher.Configuration;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using Shared.Configuration.SchemaRegistryClient.Options;
using Shared.Outbound.SchemaRegistryClient;

// Configuration
var brokerOptions = new BrokerOptions { Host = "127.0.0.1", Port = 9096 };
var schemaRegistryOptions = new SchemaRegistryClientOptions(
    new Uri("http://localhost:8081"),
    TimeSpan.FromSeconds(10));

// Schema registry client (you can use IHttpClientFactory in real apps)
var httpClientFactory = new SimpleHttpClientFactory(); // or from DI
var schemaRegistryClientFactory = new SchemaRegistryClientFactory(httpClientFactory, schemaRegistryOptions);

// Publisher
var publisherFactory = new PublisherFactory<YourMessageType>(schemaRegistryClientFactory);
var publisherOptions = new PublisherOptions(
    MessageBrokerConnectionUri: new Uri($"messageBroker://{brokerOptions.Host}:{brokerOptions.Port}"),
    SchemaRegistryConnectionUri: schemaRegistryOptions.BaseAddress,
    SchemaRegistryTimeout: schemaRegistryOptions.Timeout,
    Topic: "your-topic",
    MaxPublisherQueueSize: 10000,
    MaxSendAttempts: 5,
    MaxRetryAttempts: 5,
    BatchMaxBytes: 16384,
    BatchMaxDelay: TimeSpan.FromMilliseconds(1000));

IPublisher<YourMessageType> publisher = publisherFactory.CreatePublisher(publisherOptions);
await publisher.CreateConnectionAsync();
await publisher.PublishAsync(yourMessageInstance, cancellationToken);
```

Options and types are in `Publisher.Configuration.Options` and `Publisher.Domain.Port`. The demo project **PubSubDemo** is a full example (see `PubSubDemo/Program.cs` and `PubSubDemo/Services/MessagePublisherService.cs`).

### Subscriber

1. Use the same message type and schema as the publisher.
2. Create a schema registry client and a `SubscriberFactory<T>`.
3. Build `SubscriberOptions` (broker host/port, topic, queue size, poll interval, etc.).
4. Create a subscriber with `subscriberFactory.CreateSubscriber(options, messageHandler)` and start connection and processing.

Minimal pattern:

```csharp
using Subscriber.Configuration;
using Subscriber.Configuration.Options;
using Subscriber.Domain;

var subscriberFactory = new SubscriberFactory<YourMessageType>(schemaRegistryClient);
var subscriberOptions = new SubscriberOptions(
    MessageBrokerConnectionUri: new Uri($"messageBroker://{host}:{subscriberPort}"),
    SchemaRegistryConnectionUri: schemaRegistryBaseAddress,
    Host: host,
    Port: subscriberPort,
    Topic: "your-topic",
    MinMessageLength: 0,
    MaxMessageLength: int.MaxValue,
    MaxQueueSize: 65536,
    PollInterval: TimeSpan.FromMilliseconds(100),
    SchemaRegistryTimeout: TimeSpan.FromSeconds(10),
    MaxRetryAttempts: 3);

async Task HandleMessage(YourMessageType message)
{
    // Process message
}

ISubscriber<YourMessageType> subscriber = subscriberFactory.CreateSubscriber(subscriberOptions, HandleMessage);
await subscriber.StartConnectionAsync();
await subscriber.StartMessageProcessingAsync();
```

Broker **publisher** port (e.g. 9096) and **subscriber** port (e.g. 9098) can differ; use the subscriber port in `SubscriberOptions`. Full example: **PubSubDemo** (`PubSubDemo/Program.cs`).

---

## Running with Docker (broker and full stack)

To run the demo with Docker, see [Running the demo](#running-the-demo). The rest of this section covers extra options and details.

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose

### Start all services

From the repository root (where `compose.yaml` is):

```bash
docker compose up -d
```

This builds (if needed) and starts:

| Service         | Ports              | Description                    |
|-----------------|--------------------|--------------------------------|
| **messagebroker** | 9096, 9098, 5001 | TCP (pub/sub) + HTTP stats     |
| **schemaregistry** | 8081             | Schema Registry HTTP API       |
| **pubsubdemo**  | —                  | Publisher + Subscriber          |
| **frontend**    | 3000               | Statistics UI                   |

- Frontend: **http://localhost:3000**
- Broker statistics: **http://localhost:5001/api/statistics**
- Schema Registry: **http://localhost:8081**

### Useful commands

```bash
# Status
docker compose ps

# Logs (all or one service)
docker compose logs -f
docker compose logs -f messagebroker

# Stop
docker compose down

# Rebuild and start
docker compose up -d --build
```

### Run only the broker (and Schema Registry)

If you want to run only the broker (and optionally Schema Registry) in Docker and run the demo or your own app on the host:

```bash
docker compose up -d messagebroker schemaregistry
```

Then point your app’s broker config to `localhost` and ports `9096` (publisher) / `9098` (subscriber), and schema registry to `http://localhost:8081`.

### Configuration via environment

You can override settings with environment variables in `compose.yaml` or when running containers. Examples:

- **MessageBroker:** `MessageBrokerEnv_Server__PublisherPort`, `MessageBrokerEnv_Server__SubscriberPort`, `MessageBrokerEnv_Server__Address`
- **PubSubDemo:** `PUBSUB_Broker__Host`, `PUBSUB_Broker__Port`, `PUBSUB_Broker__SubscriberPort`, `PUBSUB_SchemaRegistry__BaseAddress`, `PUBSUB_Demo__Topic`, `PUBSUB_Demo__MessageInterval`

See `compose.yaml` and `DOCKER_SETUP.md` for more detail.

---

## Troubleshooting

### Demo cannot connect to the broker

- Ensure MessageBroker is running (`MessageBroker/src` → `dotnet run`).
- Check `PubSubDemo/appsettings.json`: `Broker:Host` and `Broker:Port` (9096) / `Broker:SubscriberPort` (9098).
- If using Docker for the broker, use `localhost` and the mapped ports, or the service name if your app runs in the same Docker network.

### Frontend shows no data

- Ensure MessageBroker is running and its HTTP API is reachable: `curl http://localhost:5001/api/statistics`.
- If the frontend runs in Docker, it uses the internal hostname `messagebroker`; from the host use `http://localhost:5001`.

### E2E or performance tests fail

- Start MessageBroker and Schema Registry first (same as the demo).
- For BddE2eTests, check `BddE2eTests/config.test.json` and that ports 9096, 9098, 8081 are correct.
- For PerformanceTests, ensure the project has a single entry point (e.g. `Program.cs` copied from `ProgramPubSub.cs` or `ProgramPubSub.cs` included in the build).

### Frontend not connected when using Docker

- The frontend (http://localhost:3000) gets statistics by calling `/api/statistics`, which nginx proxies to the MessageBroker. If you see mock/random data or “not connected”, the proxy or broker may be failing.
- **Rebuild and restart** after pulling changes: `docker compose up -d --build frontend`. The frontend image uses an nginx config that resolves the broker hostname at request time (Docker DNS).
- Check that the broker is running and exposing HTTP: `curl http://localhost:5001/api/statistics`. If that fails, check broker logs: `docker compose logs messagebroker`.
- If the broker was slow to start, refresh the page after a few seconds.

### Docker: ports already in use

- Change the port mappings in `compose.yaml` or stop other processes using 9096, 9098, 5001, 8081, 3000.

More details: **DEMO_SETUP.md**, **DOCKER_SETUP.md**, **PubSubDemo/DOCKER.md**.
