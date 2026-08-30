# Pump Hunter Android 3.0 LIVE

Pierwsza wersja aplikacji podłączana bezpośrednio do Engine 3.2.0.

## Zmiany
- prawdziwy klient HTTP do Oracle / Pump Hunter Engine,
- `/` -> realny ONLINE/OFFLINE, wersja i nazwa engine,
- `/signals` -> prawdziwe sygnały EARLY_MOVE / PUMP / COOLING / EXIT / RE_ENTRY,
- `/v31-engine` -> liczba monitorowanych aktywów,
- pełne dynamiczne okna 1m...30m w szczegółach sygnału,
- Fusion Score / Quality / Momentum / Pump Score,
- Order Flow / Volume / Context jeśli pola są dostępne,
- giełda/źródło zawsze widoczne,
- automatyczne odświeżanie co 15 sekund,
- ręczne odświeżanie,
- obsługa offline / timeout / HTTP errors,
- trwała Watchlista `★ Obserwowane` w SharedPreferences,
- własny ScoreBar bez artefaktu/kropki z Material LinearProgressIndicator,
- Android versionName = 3.0 / versionCode = 30.

## Ważne
Adres Oracle nie jest hardkodowany. Po pierwszym uruchomieniu:
1. Wejdź w Ustawienia.
2. Wpisz `http://IP_SERWERA:8000` albo adres HTTPS.
3. Kliknij `Zapisz i połącz`.

Aplikacja zapisze adres lokalnie i od tej chwili będzie łączyć się automatycznie.

## Termux
Po rozpakowaniu paczki bezpośrednio do katalogu repo:

```bash
git add -A
git status --short
git commit -m "Android 3.0 LIVE Engine 3.2.0"
git push origin main
```
