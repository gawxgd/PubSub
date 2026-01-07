# Docker Setup - Kompletna konfiguracja PubSub System

Ten dokument opisuje jak uruchomić cały system PubSub używając Docker Compose.

## Architektura

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Frontend   │────▶│ MessageBroker │◀────│ PubSubDemo  │
│  (port 3000) │     │ (port 9096,   │     │  (Publisher │
│             │     │  5001 HTTP)   │     │  +Subscriber)│
└─────────────┘     └──────────────┘     └─────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │SchemaRegistry │
                    │ (port 8081)   │
                    └──────────────┘
```

## Porty

| Serwis | Port zewnętrzny | Port wewnętrzny | Opis |
|--------|----------------|-----------------|------|
| MessageBroker | 9096 | 9096 | TCP dla Publisher/Subscriber |
| MessageBroker | 5001 | 5001 | HTTP API dla statystyk |
| SchemaRegistry | 8081 | 8080 | HTTP API dla schematów |
| Frontend | 3000 | 80 | React aplikacja |
| PubSubDemo | - | - | Tylko komunikacja wewnętrzna |

## Uruchomienie

### 1. Uruchom wszystkie serwisy

```bash
docker compose up -d
```

To uruchomi:
- **MessageBroker** - broker wiadomości (TCP: 9096, HTTP: 5001)
- **SchemaRegistry** - rejestr schematów (HTTP: 8081)
- **PubSubDemo** - demo aplikacja (Publisher + Subscriber)
- **Frontend** - interfejs użytkownika (HTTP: 3000)

### 2. Sprawdź status

```bash
docker compose ps
```

Wszystkie serwisy powinny mieć status "Up".

### 3. Sprawdź logi

```bash
# Wszystkie logi
docker compose logs -f

# Tylko MessageBroker
docker compose logs -f messagebroker

# Tylko PubSubDemo
docker compose logs -f pubsubdemo
```

### 4. Dostęp do aplikacji

- **Frontend**: http://localhost:3000
- **MessageBroker API**: http://localhost:5001/api/statistics
- **SchemaRegistry**: http://localhost:8081

## Konfiguracja

### Zmienne środowiskowe

Możesz zmienić konfigurację używając zmiennych środowiskowych w `compose.yaml`:

**MessageBroker:**
- `MessageBrokerEnv_Server__Port` - port TCP (domyślnie: 9096)
- `MessageBrokerEnv_Server__Address` - adres nasłuchiwania (domyślnie: 0.0.0.0)

**PubSubDemo:**
- `PUBSUB_Broker__Host` - host MessageBroker (domyślnie: messagebroker)
- `PUBSUB_Broker__Port` - port MessageBroker (domyślnie: 9096)
- `PUBSUB_SchemaRegistry__BaseAddress` - URL SchemaRegistry (domyślnie: http://schemaregistry:8080)
- `PUBSUB_Demo__MessageInterval` - interwał wiadomości w ms (domyślnie: 1000)
- `PUBSUB_Demo__Topic` - nazwa topiku (domyślnie: default)

**Frontend:**
- `VITE_API_URL` - URL API MessageBroker (domyślnie: http://messagebroker:5001/api)

## Zatrzymanie

```bash
# Zatrzymaj wszystkie serwisy
docker compose down

# Zatrzymaj i usuń wolumeny
docker compose down -v
```

## Rozwiązywanie problemów

### MessageBroker nie startuje

1. Sprawdź czy port 9096 i 5001 są wolne:
   ```bash
   netstat -an | grep -E "9096|5001"
   ```

2. Sprawdź logi:
   ```bash
   docker compose logs messagebroker
   ```

### Frontend nie łączy się z API

1. Sprawdź czy MessageBroker działa:
   ```bash
   curl http://localhost:5001/api/statistics
   ```

2. Sprawdź logi frontendu:
   ```bash
   docker compose logs frontend
   ```

### PubSubDemo nie wysyła wiadomości

1. Sprawdź czy MessageBroker i SchemaRegistry działają:
   ```bash
   docker compose ps
   ```

2. Sprawdź logi PubSubDemo:
   ```bash
   docker compose logs pubsubdemo
   ```

3. Sprawdź czy PubSubDemo może połączyć się z MessageBroker:
   ```bash
   docker compose exec pubsubdemo ping messagebroker
   ```

### SchemaRegistry nie odpowiada

1. Sprawdź czy port 8081 jest wolny:
   ```bash
   netstat -an | grep 8081
   ```

2. Sprawdź logi:
   ```bash
   docker compose logs schemaregistry
   ```

## Rebuild obrazów

Jeśli zmieniłeś kod i chcesz przebudować obrazy:

```bash
# Rebuild wszystkich obrazów
docker compose build

# Rebuild konkretnego serwisu
docker compose build messagebroker

# Rebuild i restart
docker compose up -d --build
```

## Development

Dla developmentu możesz uruchomić tylko backend w Dockerze, a frontend lokalnie:

```bash
# Uruchom tylko backend
docker compose up -d messagebroker schemaregistry pubsubdemo

# Uruchom frontend lokalnie
cd frontend
npm run dev
```

Frontend będzie używał proxy z `vite.config.js` do komunikacji z MessageBroker.

## Monitoring

### Sprawdź statystyki MessageBroker

```bash
curl http://localhost:5001/api/statistics | jq
```

### Sprawdź połączenia

```bash
# Lista kontenerów
docker compose ps

# Logi w czasie rzeczywistym
docker compose logs -f

# Użycie zasobów
docker stats
```

