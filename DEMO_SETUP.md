# Demo Setup - Pełne działanie z Frontendem

## Jak uruchomić pełne demo z frontendem

### Krok 1: Uruchom MessageBroker

```bash
cd MessageBroker/src
dotnet run
```

MessageBroker będzie dostępny na:
- **TCP**: port 9096 (dla Publisher/Subscriber)
- **HTTP**: port 5001 (dla statystyk i frontendu)

### Krok 2: Uruchom PubSubDemo (Publisher + Subscriber)

```bash
cd PubSubDemo
dotnet run
```

To uruchomi:
- **Publisher** - wysyła wiadomości do MessageBroker
- **Subscriber** - odbiera wiadomości z MessageBroker

### Krok 3: Uruchom Frontend

```bash
cd frontend
npm run dev
```

Frontend będzie dostępny na: **http://localhost:3000**

## Co zobaczysz

### W konsoli PubSubDemo:
- Publisher wysyła wiadomości
- Subscriber odbiera i wyświetla wiadomości

### Na frontendzie (http://localhost:3000):
- **Messages Published** - liczba wysłanych wiadomości (z commit log)
- **Messages Consumed** - liczba odebranych wiadomości
- **Active Connections** - liczba aktywnych połączeń
- **Topics** - lista topiców z ich statystykami
- Automatyczne odświeżanie co 5 sekund

## Struktura

```
Terminal 1: MessageBroker (port 9096 TCP, 5001 HTTP)
Terminal 2: PubSubDemo (Publisher + Subscriber)
Terminal 3: Frontend (port 3000)
```

## API Endpoint

Frontend łączy się z MessageBroker przez:
```
GET http://localhost:5001/api/statistics
```

Zwraca:
```json
{
  "messagesPublished": 1234,
  "messagesConsumed": 1234,
  "activeConnections": 2,
  "publisherConnections": 1,
  "subscriberConnections": 1,
  "topics": [
    {
      "name": "default",
      "messageCount": 1234,
      "lastOffset": 1234
    }
  ],
  "lastUpdate": "2024-01-01T12:00:00Z"
}
```

## Rozwiązywanie problemów

### Frontend nie pokazuje danych

1. Sprawdź czy MessageBroker działa na porcie 5001
2. Sprawdź w konsoli przeglądarki (F12) czy są błędy
3. Sprawdź czy endpoint `/api/statistics` zwraca dane:
   ```bash
   curl http://localhost:5001/api/statistics
   ```

### PubSubDemo nie może się połączyć

1. Sprawdź czy MessageBroker działa
2. Sprawdź konfigurację w `PubSubDemo/appsettings.json`
3. Sprawdź czy port 9096 jest wolny

### Brak wiadomości w frontendzie

1. Upewnij się, że PubSubDemo działa i wysyła wiadomości
2. Sprawdź czy Subscriber odbiera wiadomości w konsoli
3. Odczekaj kilka sekund - frontend odświeża się co 5 sekund

