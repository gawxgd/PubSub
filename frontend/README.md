# PubSub Statistics Dashboard

Frontend aplikacja React do wyświetlania statystyk systemu PubSub.

## Technologie

- React 18
- Vite
- Axios

## Instalacja

```bash
npm install
```

## Uruchomienie

```bash
npm run dev
```

Aplikacja będzie dostępna pod adresem: http://localhost:3000

## Build

```bash
npm run build
```

## Konfiguracja API

Domyślnie aplikacja łączy się z API pod adresem `http://localhost:5001/api`.

Możesz zmienić to ustawiając zmienną środowiskową:
```bash
VITE_API_URL=http://localhost:5001/api npm run dev
```

## Funkcjonalności

- Wyświetlanie statystyk:
  - Liczba wysłanych wiadomości
  - Liczba odebranych wiadomości
  - Aktywne połączenia
  - Lista topiców z ich statystykami
- Automatyczne odświeżanie co 5 sekund

## TODO

- [ ] Integracja z rzeczywistym API backendu
- [ ] Wykresy i wizualizacje danych
- [ ] Filtrowanie i sortowanie
- [ ] Eksport danych

