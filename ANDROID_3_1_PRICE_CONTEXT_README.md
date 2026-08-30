# Pump Hunter Android 3.1

UI refresh pod endpoint Engine 3.2.0:
`/app-price-context/{asset}`

## Nowości
- `TREND SZERSZY` na każdej karcie sygnału:
  - 1D
  - 3D
  - 5D
- zielony kolor dla wzrostu, czerwony dla spadku,
- `—` gdy okres nie jest gotowy,
- szczegóły aktywa pokazują źródło kontekstu, parę, okno referencyjne i cache,
- informacja jasno oddzielona od `RUCH TERAZ` 1m/5m/10m/30m,
- własny cache Androida 10 minut,
- maksymalnie 24 najnowsze/obserwowane aktywa są pobierane w tle,
- 4 równoległe lekkie requesty, bez blokowania głównego odświeżania sygnałów,
- błąd price-context nie przełącza całej aplikacji w OFFLINE,
- watchlista ma pierwszeństwo przy pobieraniu kontekstu.

## Backend
Oczekiwany JSON:
- `periods.1D.change_pct`
- `periods.3D.change_pct`
- `periods.5D.change_pct`
- `source_name`
- `source_symbol`
- `window_minutes`
- `cache_seconds`
- `ready`
- `affects_engine`

Android versionName: `3.1`
Android versionCode: `31`
