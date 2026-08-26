package com.revolutscanner

object ScannerStatus {

    @Volatile
    var isRunning: Boolean = false

    @Volatile
    var lastSuccessfulScan: Long = 0L

    @Volatile
    var marketsCount: Int = 0

    @Volatile
    var lastError: String? = null

    @Volatile
    var totalScans: Long = 0L

    fun scannerStarted() {
        isRunning = true
    }

    fun scannerStopped() {
        isRunning = false
    }

    fun scanSuccessful(
        marketCount: Int
    ) {
        isRunning = true
        lastSuccessfulScan = System.currentTimeMillis()
        marketsCount = marketCount
        lastError = null
        totalScans++
    }

    fun scanFailed(
        error: Throwable
    ) {
        isRunning = true

        lastError =
            error.message
                ?: error.javaClass.simpleName
                ?: "Nieznany błąd"
    }

    fun getStatusText(): String {

        return when {

            !isRunning ->
                "🔴 Scanner zatrzymany"

            lastError != null ->
                "🔴 Błąd API"

            lastSuccessfulScan == 0L ->
                "🟡 Oczekiwanie na pierwszy odczyt..."

            else ->
                "🟢 Scanner działa"
        }
    }
}
