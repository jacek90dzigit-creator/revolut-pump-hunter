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

    @Volatile
    var backoffUntil: Long = 0L

    @Volatile
    var isBackoff: Boolean = false

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

        isBackoff = false
        backoffUntil = 0L
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

    fun startBackoff(
        durationMs: Long
    ) {
        isBackoff = true

        backoffUntil =
            System.currentTimeMillis() +
                durationMs
    }

    fun getRemainingBackoffSeconds(): Long {

        if (!isBackoff) {
            return 0L
        }

        val remaining =
            backoffUntil -
                System.currentTimeMillis()

        if (remaining <= 0L) {
            return 0L
        }

        return remaining / 1000L
    }

    fun getStatusText(): String {

        return when {

            !isRunning ->
                "🔴 Scanner zatrzymany"

            isBackoff &&
                getRemainingBackoffSeconds() > 0 ->
                "🟡 Revolut: BACKOFF"

            lastError != null ->
                "🔴 Błąd API"

            lastSuccessfulScan == 0L ->
                "🟡 Oczekiwanie na pierwszy odczyt..."

            else ->
                "🟢 Scanner działa"
        }
    }
}
