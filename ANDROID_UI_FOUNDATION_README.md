# Android 2.0 UI Foundation

Pakiet przygotowany dla `revolut-pump-hunter`.

## Co zmienia
- Jetpack Compose + Material 3
- nowy dashboard
- ekrany: Home, Sygnały, Aktywne, Historia, Ustawienia
- warstwy `data`, `domain`, `ui`
- mockowane dane do niezależnego rozwijania wyglądu
- kontrakt `PumpHunterApi` pod późniejsze połączenie z backendem 3.1.2
- lokalny ScannerService nie jest już deklarowany ani uruchamiany

## Jak użyć
Skopiuj pliki z paczki do repo, zachowując ścieżki. Istniejące pliki o tych samych nazwach zastąp w całości.

Stare klasy `ScannerService.kt`, `RevolutApi.kt`, `PumpEngine.kt`, `ExitEngine.kt`, `PumpHistory.kt`,
`PumpChartView.kt` i `ScannerStatus.kt` mogą na razie zostać w repo. Nie są używane przez nowy UI.
