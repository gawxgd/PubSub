# 🚀 Quick Start - Performance Tests

## Szybkie uruchomienie testów wydajnościowych

### Krok 1: Uruchom MessageBroker i SchemaRegistry

**Opcja A: Docker Compose (najszybsze)**
```powershell
docker compose up -d messagebroker schemaregistry
```

**Opcja B: Ręcznie (2 terminale)**

**Terminal 1 - MessageBroker:**
```powershell
cd MessageBroker\src
dotnet run
```

**Terminal 2 - SchemaRegistry:**
```powershell
cd SchemaRegistry\src
$env:ASPNETCORE_URLS="http://localhost:8081"
dotnet run
```

### Krok 2: Uruchom testy wydajnościowe

**Terminal 3 - Performance Tests:**
```powershell
cd PerformanceTests
dotnet run
```

## Co się stanie?

1. ✅ Testy sprawdzą dostępność MessageBroker i SchemaRegistry
2. ✅ Zarejestrują schemat dla `TestMessage`
3. ✅ Uruchomią 3 scenariusze testowe:
   - **Publisher Throughput** - test przepustowości publikowania
   - **End-to-End Throughput** - test całego systemu (publikowanie + odbieranie)
   - **Publisher Latency** - test latencji publikowania
4. ✅ Wygenerują raporty w folderze `reports/`

## Wyniki

Po zakończeniu testów znajdziesz raporty w:
- `PerformanceTests/reports/[data]/nbomber_report_*.html` - **interaktywny raport HTML** (otwórz w przeglądarce!)
- `PerformanceTests/reports/[data]/nbomber_report_*.csv` - dane do analizy
- `PerformanceTests/reports/[data]/nbomber_report_*.txt` - tekstowy raport

### Jak otworzyć raport HTML:

**Opcja 1: Automatycznie (PowerShell)**
```powershell
# Otwórz najnowszy raport w domyślnej przeglądarce
Get-ChildItem -Path "PerformanceTests\reports" -Filter "*.html" -Recurse | 
    Sort-Object LastWriteTime -Descending | 
    Select-Object -First 1 | 
    ForEach-Object { Start-Process $_.FullName }
```

**Opcja 2: Ręcznie**
1. Przejdź do folderu `PerformanceTests/reports/`
2. Znajdź najnowszy folder z datą (np. `2026-01-04--16-38-14_session_xxx`)
3. Otwórz plik `nbomber_report_*.html` w przeglądarce (podwójne kliknięcie)

**Opcja 3: Z linii poleceń**
```powershell
# Znajdź najnowszy raport
$latestReport = Get-ChildItem -Path "PerformanceTests\reports" -Filter "*.html" -Recurse | 
    Sort-Object LastWriteTime -Descending | 
    Select-Object -First 1

# Otwórz w przeglądarce
Start-Process $latestReport.FullName
```

## Konfiguracja (opcjonalna)

Możesz zmienić konfigurację używając zmiennych środowiskowych:

```powershell
$env:BROKER_HOST="localhost"
$env:BROKER_PORT="9096"
$env:SCHEMA_REGISTRY_URL="http://localhost:8081"
$env:TOPIC="my-test-topic"
dotnet run
```

## Rozwiązywanie problemów

### ❌ "MessageBroker is not running!"
- Upewnij się, że MessageBroker działa na porcie 9096
- Sprawdź: `netstat -an | findstr 9096`

### ❌ "Cannot connect to SchemaRegistry"
- Upewnij się, że SchemaRegistry działa na porcie 8081
- Sprawdź: `netstat -an | findstr 8081`

### ⚠️ "Failed to register schema"
- To może być normalne, jeśli schemat już istnieje
- Testy spróbują kontynuować

## Czas trwania testów

- Warm-up: 3 sekundy na scenariusz
- Publisher Throughput: ~18 sekund (3s warm-up + 15s test)
- End-to-End Throughput: ~23 sekundy (3s warm-up + 20s test)
- Publisher Latency: ~18 sekund (3s warm-up + 15s test)

**Łączny czas: ~1 minuta** (znacznie skrócone dla szybszych testów)

