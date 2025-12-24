# Performance Tests dla PubSub System

Projekt zawiera testy wydajnościowe dla systemu PubSub używające NBomber.

## Wymagania

- .NET 9.0
- Uruchomiony MessageBroker (domyślnie na porcie 9096)
- Uruchomiony SchemaRegistry (domyślnie na porcie 5002)

## Uruchomienie MessageBroker i SchemaRegistry

**Przed uruchomieniem testów wydajnościowych musisz uruchomić MessageBroker i SchemaRegistry:**

### Opcja 1: Docker Compose (zalecane)

```bash
# Uruchom MessageBroker i SchemaRegistry
docker compose up -d messagebroker schemaregistry

# Sprawdź czy działają
docker compose ps
```

### Opcja 2: Ręczne uruchomienie

**Terminal 1 - MessageBroker:**
```bash
cd MessageBroker/src
dotnet run
```

**Terminal 2 - SchemaRegistry:**
```bash
cd SchemaRegistry/src
dotnet run
```

**Uwaga:** Testy automatycznie sprawdzają dostępność MessageBroker przed uruchomieniem i wyświetlą komunikat błędu jeśli broker nie jest dostępny.

## Konfiguracja

Testy można skonfigurować za pomocą zmiennych środowiskowych:

- `BROKER_HOST` - host brokera (domyślnie: localhost)
- `BROKER_PORT` - port brokera (domyślnie: 9096)
- `SCHEMA_REGISTRY_URL` - URL Schema Registry (domyślnie: http://localhost:5002)
- `TOPIC` - nazwa topiku (domyślnie: performance-test)

## Scenariusze testowe

### 1. Publisher Throughput (`publisher_throughput`)
Test przepustowości publikowania wiadomości.
- Warm-up: 5 sekund
- Load: Ramp up od 10 do 100 wiadomości/sekundę przez 30 sekund
- Mierzy: liczbę wiadomości na sekundę, które można opublikować

### 2. End-to-End Throughput (`end_to_end_throughput`)
Test end-to-end: publikowanie i odbieranie wiadomości.
- Warm-up: 5 sekund
- Load: Stałe 50 wiadomości/sekundę przez 60 sekund
- Mierzy: przepustowość całego systemu, utratę wiadomości

### 3. Publisher Latency (`publisher_latency`)
Test latencji publikowania pojedynczej wiadomości.
- Warm-up: 5 sekund
- Load: Stałe 10 wiadomości/sekundę przez 30 sekund
- Mierzy: czas opublikowania pojedynczej wiadomości

## Uruchomienie

```bash
# Uruchom wszystkie testy
dotnet run --project PerformanceTests

# Lub z konkretną konfiguracją
BROKER_HOST=localhost BROKER_PORT=9096 dotnet run --project PerformanceTests
```

## Wyniki

Wyniki testów są zapisywane w folderze `reports/` w formatach:
- HTML (interaktywny raport)
- CSV (dane do analizy)
- TXT (tekstowy raport)

## Przykładowe użycie

```bash
# Uruchom testy z domyślną konfiguracją
dotnet run --project PerformanceTests

# Uruchom testy z niestandardową konfiguracją
$env:BROKER_HOST="192.168.1.100"
$env:BROKER_PORT="9096"
$env:TOPIC="my-test-topic"
dotnet run --project PerformanceTests
```

