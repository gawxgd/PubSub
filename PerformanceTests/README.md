# Performance Tests dla PubSub System

Projekt zawiera testy wydajnościowe dla systemu PubSub używające NBomber. Testy mogą również mierzyć wydajność Kafki dla porównania.

## Wymagania

- .NET 9.0
- Uruchomiony MessageBroker (domyślnie na porcie 9096)
- Uruchomiony SchemaRegistry (domyślnie na porcie 8081)
- (Opcjonalnie) Kafka i Confluent Schema Registry dla testów porównawczych

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

### PubSub System (wymagane)
- `BROKER_HOST` - host brokera (domyślnie: localhost)
- `BROKER_PORT` - port brokera (domyślnie: 9096)
- `SCHEMA_REGISTRY_URL` - URL Schema Registry (domyślnie: http://localhost:8081)
- `TOPIC` - nazwa topiku (domyślnie: performance-test)

### Kafka (opcjonalne - dla testów porównawczych)
- `ENABLE_KAFKA_TESTS` - włącz testy Kafki (domyślnie: false, ustaw na "true" aby włączyć)
- `KAFKA_BOOTSTRAP_SERVERS` - adresy serwerów Kafki (domyślnie: localhost:9092)
- `KAFKA_SCHEMA_REGISTRY_URL` - URL Confluent Schema Registry (domyślnie: http://localhost:8081)
- `KAFKA_TOPIC` - nazwa topiku Kafki (domyślnie: {TOPIC}-kafka)
- `KAFKA_CONSUMER_GROUP_ID` - ID grupy konsumentów (domyślnie: performance-test-consumer-group)

## Scenariusze testowe

### PubSub System

#### 1. Publisher Throughput (`publisher_throughput`)
Test przepustowości publikowania wiadomości.
- Warm-up: 3 sekundy
- Load: Stałe 10 wiadomości/sekundę przez 15 sekund
- Mierzy: liczbę wiadomości na sekundę, które można opublikować

#### 2. End-to-End Throughput (`end_to_end_throughput`)
Test end-to-end: publikowanie i odbieranie wiadomości.
- Warm-up: 3 sekundy
- Load: Stałe 20 wiadomości/sekundę przez 20 sekund
- Mierzy: przepustowość całego systemu, utratę wiadomości

#### 3. Publisher Latency (`publisher_latency`)
Test latencji publikowania pojedynczej wiadomości.
- Warm-up: 3 sekundy
- Load: Stałe 10 wiadomości/sekundę przez 15 sekund
- Mierzy: czas opublikowania pojedynczej wiadomości

### Kafka (tylko gdy `ENABLE_KAFKA_TESTS=true`)

#### 4. Kafka Publisher Throughput (`kafka_publisher_throughput`)
Test przepustowości publikowania wiadomości do Kafki.
- Warm-up: 3 sekundy
- Load: Stałe 10 wiadomości/sekundę przez 15 sekund
- Mierzy: liczbę wiadomości na sekundę, które można opublikować do Kafki

#### 5. Kafka End-to-End Throughput (`kafka_end_to_end_throughput`)
Test end-to-end dla Kafki: publikowanie i odbieranie wiadomości.
- Warm-up: 3 sekundy
- Load: Stałe 20 wiadomości/sekundę przez 20 sekund
- Mierzy: przepustowość całego systemu Kafki, utratę wiadomości

#### 6. Kafka Publisher Latency (`kafka_publisher_latency`)
Test latencji publikowania pojedynczej wiadomości do Kafki.
- Warm-up: 3 sekundy
- Load: Stałe 10 wiadomości/sekundę przez 15 sekund
- Mierzy: czas opublikowania pojedynczej wiadomości do Kafki

## Uruchomienie

### PowerShell (Windows)

```powershell
# Uruchom wszystkie testy z domyślną konfiguracją
cd PerformanceTests
dotnet run

# Lub z konkretną konfiguracją
$env:BROKER_HOST="localhost"
$env:BROKER_PORT="9096"
$env:SCHEMA_REGISTRY_URL="http://localhost:8081"
$env:TOPIC="performance-test"
dotnet run
```

### Bash/Linux/Mac

```bash
# Uruchom wszystkie testy
cd PerformanceTests
dotnet run

# Lub z konkretną konfiguracją
BROKER_HOST=localhost BROKER_PORT=9096 SCHEMA_REGISTRY_URL=http://localhost:8081 TOPIC=performance-test dotnet run
```

## Wyniki

Wyniki testów są zapisywane w folderze `reports/` w formatach:
- HTML (interaktywny raport)
- CSV (dane do analizy)
- TXT (tekstowy raport)

## Przykładowe użycie

### PowerShell (Windows)

```powershell
# 1. Uruchom MessageBroker i SchemaRegistry (w osobnych terminalach)
# Terminal 1:
cd MessageBroker\src
dotnet run

# Terminal 2:
cd SchemaRegistry\src
$env:ASPNETCORE_URLS="http://localhost:8081"
dotnet run

# 2. W trzecim terminalu uruchom testy wydajnościowe
cd PerformanceTests
dotnet run
```

### Pełny przykład z konfiguracją

```powershell
# Ustaw zmienne środowiskowe
$env:BROKER_HOST="localhost"
$env:BROKER_PORT="9096"
$env:SCHEMA_REGISTRY_URL="http://localhost:8081"
$env:TOPIC="my-performance-test"

# Uruchom testy
cd PerformanceTests
dotnet run
```

### Uruchomienie z testami Kafki (porównanie)

```powershell
# Ustaw zmienne środowiskowe dla PubSub
$env:BROKER_HOST="localhost"
$env:BROKER_PORT="9096"
$env:SCHEMA_REGISTRY_URL="http://localhost:8081"
$env:TOPIC="performance-test"

# Włącz testy Kafki
$env:ENABLE_KAFKA_TESTS="true"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:KAFKA_SCHEMA_REGISTRY_URL="http://localhost:8081"
$env:KAFKA_TOPIC="performance-test-kafka"

# Uruchom testy (PubSub + Kafka)
cd PerformanceTests
dotnet run
```

**Uwaga:** Przed uruchomieniem testów Kafki upewnij się, że:
1. Kafka jest uruchomiona (np. przez Docker Compose)
2. Confluent Schema Registry jest dostępny (może być ten sam co dla PubSub)
3. Topic w Kafce zostanie utworzony automatycznie przy pierwszym publish

## Co testy robią?

1. **Sprawdzają dostępność** MessageBroker i SchemaRegistry przed uruchomieniem
2. **Rejestrują schemat** dla `TestMessage` w SchemaRegistry
3. **Uruchamiają scenariusze testowe PubSub:**
   - `publisher_throughput` - test przepustowości publikowania
   - `end_to_end_throughput` - test end-to-end (publikowanie + odbieranie)
   - `publisher_latency` - test latencji publikowania
4. **Jeśli włączone (`ENABLE_KAFKA_TESTS=true`), uruchamiają również testy Kafki:**
   - Rejestrują schemat w Confluent Schema Registry
   - `kafka_publisher_throughput` - test przepustowości publikowania do Kafki
   - `kafka_end_to_end_throughput` - test end-to-end dla Kafki
   - `kafka_publisher_latency` - test latencji publikowania do Kafki
5. **Generują raporty** w folderze `reports/` (HTML, CSV, TXT) z wynikami wszystkich testów

## Porównanie wyników

Raporty NBomber zawierają szczegółowe metryki dla każdego scenariusza, co pozwala na bezpośrednie porównanie wydajności:
- **Throughput** - liczba wiadomości na sekundę
- **Latency** - czas odpowiedzi (min, średnia, max, percentyle)
- **Success rate** - procent udanych operacji
- **Message loss** - dla testów E2E

Wszystkie metryki są dostępne w raportach HTML, CSV i TXT w folderze `reports/`.

