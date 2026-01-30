# PubSub

A .NET publisher subscriber system.

---

## Table of contents

- [Running the demo](#running-the-demo)
- [Running tests](#running-tests)
- [Using the Publisher and Subscriber](#using-the-publisher-and-subscriber)
- [Configuring the broker](#configuring-the-broker)
- [Running with Docker](#running-with-docker)

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

## Using the Publisher and Subscriber

Add project references to **Publisher**, **Subscriber**, **LoggerLib**, and **Shared** (Publisher and Subscriber bring in MessageBroker and Shared transitively).

Read configuration from `appsettings.json` (e.g. broker host, publisher port, subscriber port, max queue size, and Schema Registry base address and timeout) and optionally environment variables; use those values when building `PublisherOptions` and `SubscriberOptions`. The Publisher and Subscriber projects do not define a broker config class—you supply the values from your own configuration.

### Publisher

Create a schema registry client factory (`SchemaRegistryClientFactory` with `IHttpClientFactory` and `SchemaRegistryClientOptions`), then a `PublisherFactory<T>` for your message type. Build `PublisherOptions` with broker URI `messageBroker://{host}:{port}`, schema registry URI and timeout, topic, `MaxPublisherQueueSize`, `MaxSendAttempts`, `MaxRetryAttempts`, `BatchMaxBytes`, and `BatchMaxDelay`. Create the publisher with `publisherFactory.CreatePublisher(publisherOptions)`, then `await publisher.CreateConnection()` and `await publisher.PublishAsync(message, cancellationToken)`.

```csharp
var schemaRegistryClientFactory = new SchemaRegistryClientFactory(httpClientFactory, schemaRegistryOptions);
var publisherFactory = new PublisherFactory<YourMessageType>(schemaRegistryClientFactory);

var publisherOptions = new PublisherOptions(
    MessageBrokerConnectionUri: new Uri($"messageBroker://{host}:{publisherPort}"),
    SchemaRegistryConnectionUri: schemaRegistryBaseAddress,
    SchemaRegistryTimeout: schemaRegistryTimeout,
    Topic: "your-topic",
    MaxPublisherQueueSize: 10000,
    MaxSendAttempts: 5,
    MaxRetryAttempts: 5,
    BatchMaxBytes: 16384,
    BatchMaxDelay: TimeSpan.FromMilliseconds(1000));

var publisher = publisherFactory.CreatePublisher(publisherOptions);
await publisher.CreateConnection();
await publisher.PublishAsync(message, cancellationToken);
```

### Subscriber

Create a `SubscriberFactory<T>` with the schema registry client (from `schemaRegistryClientFactory.Create()`). Build `SubscriberOptions` with `MessageBrokerConnectionUri` using the **subscriber** port (e.g. 9098), `SchemaRegistryConnectionUri`, Host, Port (subscriber port), Topic, `MinMessageLength: 0`, `MaxMessageLength: int.MaxValue`, `MaxQueueSize: 65536`, `PollInterval`, `SchemaRegistryTimeout`, and `MaxRetryAttempts: 3`. Create the subscriber with `subscriberFactory.CreateSubscriber(subscriberOptions, messageHandler)`, then `await subscriber.StartConnectionAsync()` and `await subscriber.StartMessageProcessingAsync()`.

```csharp
var schemaRegistryClient = schemaRegistryClientFactory.Create();
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
    SchemaRegistryTimeout: schemaRegistryTimeout,
    MaxRetryAttempts: 3);

var subscriber = subscriberFactory.CreateSubscriber(subscriberOptions, async msg => { /* handle */ });
await subscriber.StartConnectionAsync();
await subscriber.StartMessageProcessingAsync();
```

The broker exposes a **publisher** port (default 9096) and a **subscriber** port (default 9098); use the subscriber port in `SubscriberOptions`.

---

## Configuring the broker

By default the broker reads `config.json` from its working directory. You can override settings without changing the file:

- **Environment variables** (prefix `MessageBrokerEnv_`): e.g. `MessageBrokerEnv_Server__PublisherPort`, `MessageBrokerEnv_Server__SubscriberPort`, `MessageBrokerEnv_Server__Address`, `MessageBrokerEnv_LoggerPort`. These take precedence over `config.json`.
- **config.json** (in the broker working directory): top-level `LoggerPort` (HTTP statistics API, default 5001); section `Server` with `PublisherPort` (default 9096), `SubscriberPort` (default 9098), `Address` (bind address, default 127.0.0.1), and other TCP/commit-log options.

Use non-default ports or bind address by setting the env vars or by editing `config.json` before starting the broker.

## Running with Docker

To run the **MessageBroker** and **Schema Registry** with default options (publisher port 9096, subscriber port 9098, HTTP stats on 5001):

```bash
docker compose up -d messagebroker schemaregistry
```

Configure your app with broker host `localhost`, publisher port `9096`, subscriber port `9098`, and Schema Registry at `http://localhost:8081`. To use different ports, set the broker environment variables in `compose.yaml` (e.g. `MessageBrokerEnv_Server__PublisherPort`, `MessageBrokerEnv_Server__SubscriberPort`) and adjust your app and port mappings accordingly.
