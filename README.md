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

From the root folder, run:

```bash
cd PubSubDemo
docker compose up -d
```

This starts the **MessageBroker**, **Schema Registry**, **PubSubDemo** (Publisher + Subscriber), and the **Frontend**. Open **http://localhost:3000** to see the statistics dashboard (messages published/consumed, connections, topics).

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

### Referencing the projects

Add project references to **Publisher**, **Subscriber**, **LoggerLib**, and **Shared**. Publisher and Subscriber reference MessageBroker and Shared transitively, so you do not need to reference those explicitly.

```xml
<ItemGroup>
  <ProjectReference Include="path/to/Publisher/Publisher.csproj" />
  <ProjectReference Include="path/to/Subscriber/Subscriber.csproj" />
  <ProjectReference Include="path/to/LoggerLib/LoggerLib.csproj" />
  <ProjectReference Include="path/to/Shared/Shared.csproj" />
</ItemGroup>
```

### Publisher

1. Define your message type (a class that will be serialized with Avro). The schema is auto-registered in the Schema Registry when you publish.
2. Create `IHttpClientFactory` (e.g. `SimpleHttpClientFactory` or from DI) and `SchemaRegistryClientOptions` (Schema Registry base address and timeout).
3. Create a schema registry client factory: `SchemaRegistryClientFactory` with that `IHttpClientFactory` and `SchemaRegistryClientOptions`.
4. Create a `PublisherFactory<T>` for your message type.
5. Build `PublisherOptions` with broker URI `messageBroker://{host}:{port}`, schema registry URI and timeout, topic, `MaxPublisherQueueSize`, `MaxSendAttempts`, `MaxRetryAttempts`, `BatchMaxBytes`, and `BatchMaxDelay`.
6. Create the publisher with `publisherFactory.CreatePublisher(publisherOptions)`.
7. Call `await publisher.CreateConnection()` then `await publisher.PublishAsync(message, cancellationToken)`.

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

1. Use the same message type and schema as the publisher (the subscriber deserializes with Avro using the Schema Registry; the message type must match what the publisher sends).
2. Ensure you have a schema registry client factory (same `SchemaRegistryClientFactory` as for the publisher, from `IHttpClientFactory` and `SchemaRegistryClientOptions`).
3. Create a schema registry client with `schemaRegistryClientFactory.Create()`.
4. Define a message handler: a `Func<T, Task>` that will be invoked for each received message (e.g. `async msg => { /* process msg */ }`).
5. Create a `SubscriberFactory<T>` with that schema registry client.
6. Build `SubscriberOptions` with `MessageBrokerConnectionUri` using the **subscriber** port (e.g. 9098), `SchemaRegistryConnectionUri`, Host, Port (subscriber port), Topic, `MinMessageLength: 0`, `MaxMessageLength: int.MaxValue`, `MaxQueueSize: 65536`, `PollInterval`, `SchemaRegistryTimeout`, and `MaxRetryAttempts: 3`.
7. Create the subscriber with `subscriberFactory.CreateSubscriber(subscriberOptions, messageHandler)`.
8. Call `await subscriber.StartConnectionAsync()` then `await subscriber.StartMessageProcessingAsync()`.

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

You can use the defaults from [MessageBroker/src/config.json](MessageBroker/src/config.json) or change that file to use different ports or settings. If you edit the file, rebuild the broker project and then run Docker so the new config is used.

## Running with Docker

To run the **MessageBroker** and **Schema Registry**:

```bash
docker compose up -d messagebroker schemaregistry
```

