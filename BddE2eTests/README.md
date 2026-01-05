# BDD E2E Tests

Testy end-to-end oparte na BDD (Behavior-Driven Development) używające Reqnroll (SpecFlow) i NUnit.

## Wymagania

- .NET 9.0 SDK
- Porty 9096, 9098 i 8081 muszą być wolne (testy automatycznie uruchamiają MessageBroker i SchemaRegistry)

## Uruchomienie

### Podstawowe uruchomienie

```powershell
cd BddE2eTests
dotnet test
```

### Z dodatkowymi opcjami

```powershell
# Uruchom wszystkie testy z szczegółowym outputem
dotnet test --verbosity normal

# Uruchom konkretny feature
dotnet test --filter "FullyQualifiedName~PublisherSubscriber"

# Uruchom konkretny scenariusz
dotnet test --filter "FullyQualifiedName~PublisherSubscriber.Communication"

# Uruchom z logowaniem do pliku
dotnet test --logger "console;verbosity=detailed" --logger "trx;LogFileName=test-results.trx"
```

### Z Visual Studio / Rider

1. Otwórz rozwiązanie `PubSub.sln`
2. Przejdź do Test Explorer
3. Znajdź projekt `BddE2eTests`
4. Kliknij prawym przyciskiem i wybierz "Run All Tests"

## Jak działają testy?

Testy automatycznie:

1. **Przed każdym scenariuszem** (`[BeforeScenario]`):
   - Tworzą tymczasowy katalog dla commit log
   - Uruchamiają MessageBroker na porcie 9096 (Publisher) i 9098 (Subscriber)
   - Uruchamiają SchemaRegistry na porcie 8081
   - Rejestrują schematy testowe dla topiców: `default`, `test-topic`, `custom-topic`
   - Czekają na pełne uruchomienie serwisów (timeout: 30 sekund)

2. **Podczas testu**:
   - Używają konfiguracji z `config.test.json`
   - Tworzą Publisher i Subscriber z odpowiednimi opcjami
   - Wykonują scenariusze BDD zdefiniowane w plikach `.feature`

3. **Po każdym scenariuszu** (`[AfterScenario]`):
   - Zatrzymują SchemaRegistry
   - Zatrzymują MessageBroker
   - Usuwają tymczasowe katalogi (commit log, schema store)

## Konfiguracja

Konfiguracja testów znajduje się w pliku `config.test.json`:

```json
{
  "Server": {
    "PublisherPort": 9096,
    "SubscriberPort": 9098,
    "Address": "127.0.0.1"
  },
  "Publisher": {
    "BrokerHost": "127.0.0.1",
    "BrokerPort": 9096,
    "Topic": "test-topic"
  },
  "Subscriber": {
    "BrokerHost": "127.0.0.1",
    "BrokerPort": 9098,
    "Topic": "test-topic"
  },
  "SchemaRegistry": {
    "Host": "127.0.0.1",
    "Port": 8081
  }
}
```

## Dostępne scenariusze

### PublisherSubscriber.feature

- **Publisher sends message and subscriber receives it** - Podstawowy test komunikacji
- **Ordered delivery per partition** - Test kolejności dostarczania wiadomości

### PublishingToNonExistentTopic.feature

- Testy związane z publikowaniem do nieistniejących topiców

## Rozwiązywanie problemów

### ❌ Błąd MSB4216: Could not run the "ReplaceTokenInFileTask" task

**Błąd:** `error MSB4216: Could not run the "ReplaceTokenInFileTask" task because MSBuild could not create or connect to a task host`

**Przyczyna:** Znany problem z Reqnroll 2.3.0 i .NET 9.0 SDK związany z MSBuild task host.

**Rozwiązania:**

1. **Użyj Visual Studio lub Rider** (zalecane):
   - Otwórz rozwiązanie w Visual Studio 2022 lub JetBrains Rider
   - Uruchom testy z Test Explorer
   - IDE używa własnego MSBuild, który zazwyczaj działa poprawnie

2. **Użyj MSBuild zamiast dotnet build**:
   ```powershell
   # Znajdź ścieżkę do MSBuild
   $msbuild = "C:\Program Files\Microsoft Visual Studio\2022\Community\MSBuild\Current\Bin\MSBuild.exe"
   
   # Zbuduj projekt
   & $msbuild BddE2eTests.csproj /t:Build
   
   # Uruchom testy
   dotnet test --no-build
   ```

3. **Tymczasowe obejście - użyj już zbudowanego projektu**:
   ```powershell
   # Jeśli projekt był już wcześniej zbudowany w IDE
   dotnet test --no-build
   ```

4. **Zaktualizuj Reqnroll** (jeśli dostępna nowsza wersja):
   ```powershell
   # Sprawdź dostępne wersje
   dotnet add package Reqnroll --version
   ```

### ❌ Port już w użyciu

**Błąd:** `Failed to bind to address` lub `Port already in use`

**Rozwiązanie:**
```powershell
# Sprawdź czy porty są wolne
netstat -an | findstr "9096 9098 8081"

# Zatrzymaj procesy używające portów (Windows)
Get-NetTCPConnection -LocalPort 9096,9098,8081 | 
    Select-Object -ExpandProperty OwningProcess | 
    Stop-Process -Force
```

### ❌ Timeout podczas uruchamiania brokera

**Błąd:** `Broker startup timed out after 30 seconds`

**Rozwiązanie:**
- Sprawdź czy porty są wolne
- Sprawdź logi w `TestContext.Progress` output
- Zwiększ timeout w `TestBase.cs` (metoda `WaitForBrokerStartupAsync`)

### ❌ Testy nie znajdują pliku config.test.json

**Błąd:** `Configuration file not found`

**Rozwiązanie:**
- Upewnij się, że plik `config.test.json` istnieje w katalogu `BddE2eTests`
- Sprawdź czy plik jest skopiowany do output directory (jest w `.csproj` jako `CopyToOutputDirectory`)

### ❌ Schema registration failed

**Błąd:** `Failed to register schema for topic`

**Rozwiązanie:**
- Sprawdź czy SchemaRegistry uruchomił się poprawnie
- Sprawdź logi w output testów
- Sprawdź czy port 8081 jest dostępny

## Debugowanie

### Uruchomienie z debuggerem

1. W Visual Studio / Rider:
   - Ustaw breakpoint w kodzie testu
   - Kliknij prawym przyciskiem na test i wybierz "Debug"

2. Z linii poleceń:
```powershell
# Uruchom z debuggerem (wymaga VS Code lub innego debuggera)
dotnet test --logger "console;verbosity=detailed"
```

### Szczegółowe logi

Testy używają `TestContext.Progress.WriteLine()` do logowania. Wszystkie logi są widoczne w output testów:

```powershell
dotnet test --verbosity detailed
```

## Struktura projektu

```
BddE2eTests/
├── Features/              # Pliki .feature z scenariuszami BDD
│   ├── PublisherSubscriber.feature
│   └── PublishingToNonExistentTopic.feature
├── Steps/                 # Implementacje kroków BDD
│   ├── Publisher/
│   └── Subscriber/
├── Configuration/         # Konfiguracja i builders
├── TestBase.cs           # Setup/teardown dla scenariuszy
└── config.test.json      # Konfiguracja testów
```

## Przykładowy output

```
Starting test execution, please wait...
A total of 1 test files matched the specified pattern.

[TestBase] === Starting new scenario setup ===
[TestBase] Creating broker host...
[TestBase] Initializing logger...
[TestBase] Starting broker...
[TestBase] Waiting for broker startup...
[TestBase] MessageBroker started
[TestBase] Creating schema registry...
[TestBase] Starting schema registry...
[TestBase] SchemaRegistry started on port 8081
[TestBase] Registering test schemas...
[TestBase] Registered schema for topic: default
[TestBase] Registered schema for topic: test-topic
[TestBase] Registered schema for topic: custom-topic
[TestBase] Setup complete!

Passed!  - Failed:     0, Passed:     2, Skipped:     0, Total:     2, Duration: 5 s
```

## Uwagi

- Testy automatycznie zarządzają MessageBroker i SchemaRegistry - **nie uruchamiaj ich ręcznie**
- Każdy scenariusz uruchamia własną instancję brokera (izolacja testów)
- Tymczasowe katalogi są automatycznie czyszczone po testach
- Porty muszą być wolne przed uruchomieniem testów

