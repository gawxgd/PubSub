# Jak uruchomić frontend niezależnie

## Wymagania

- Node.js (wersja 18 lub nowsza)
- npm (zazwyczaj dołączony do Node.js)

## Sprawdzenie wersji

```bash
node --version
npm --version
```

## Instalacja zależności (tylko raz)

```bash
cd frontend
npm install
```

## Uruchomienie w trybie deweloperskim

```bash
cd frontend
npm run dev
```

Aplikacja będzie dostępna pod adresem: **http://localhost:3000**

## Uruchomienie zbudowanej wersji produkcyjnej

```bash
cd frontend
npm run build
npm run preview
```

## Konfiguracja API

Frontend domyślnie próbuje połączyć się z API pod adresem `http://localhost:5001/api`.

Jeśli chcesz zmienić adres API, utwórz plik `.env` w folderze `frontend`:

```env
VITE_API_URL=http://localhost:5001/api
```

Lub uruchom z zmienną środowiskową:

```bash
# Windows PowerShell
$env:VITE_API_URL="http://localhost:5001/api"; npm run dev

# Windows CMD
set VITE_API_URL=http://localhost:5001/api && npm run dev

# Linux/Mac
VITE_API_URL=http://localhost:5001/api npm run dev
```

## Działanie bez backendu

Frontend automatycznie używa **mockowych danych**, jeśli API nie jest dostępne. 
Nie musisz uruchamiać backendu - aplikacja będzie działać z przykładowymi danymi.

## Zatrzymanie serwera

Naciśnij `Ctrl+C` w terminalu, gdzie działa serwer.

## Rozwiązywanie problemów

### Port 3000 jest zajęty

Vite automatycznie zaproponuje inny port (np. 3001). Sprawdź output w terminalu.

### Błędy instalacji

```bash
# Usuń node_modules i zainstaluj ponownie
rm -rf node_modules package-lock.json
npm install
```

### Błędy kompilacji

```bash
# Sprawdź czy wszystkie pliki są na miejscu
npm run build
```

