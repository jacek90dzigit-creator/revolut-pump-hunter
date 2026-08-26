package com.revolutscanner

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.min

class ScannerService : Service() {

    companion object {
        const val CHANNEL_ID = "pump_hunter_scanner"
        const val ALERT_CHANNEL_ID = "pump_hunter_alerts"
        const val EXIT_CHANNEL_ID = "pump_hunter_exit"
        const val NOTIFICATION_ID = 101

        private const val NORMAL_SCAN_DELAY_MS = 90_000L
        private const val MAX_BACKOFF_MS = 10 * 60_000L
    }

    private val scheduler =
        Executors.newSingleThreadScheduledExecutor()

    private val lastPumpAlertTime =
        mutableMapOf<String, Long>()

    private val lastExitAlertTime =
        mutableMapOf<String, Long>()

    private var currentBackoffMs =
        NORMAL_SCAN_DELAY_MS

    private var stopped = false

    private var wakeLock: PowerManager.WakeLock? = null

    override fun onCreate() {
        super.onCreate()

        ScannerStatus.scannerStarted()

        createNotificationChannels()
        acquireWakeLock()

        val notification: Notification =
            NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("🔥 Revolut Pump Hunter")
                .setContentText("Scanner rynku działa w tle")
                .setSmallIcon(android.R.drawable.ic_popup_sync)
                .setOngoing(true)
                .setPriority(NotificationCompat.PRIORITY_LOW)
                .build()

        startForeground(
            NOTIFICATION_ID,
            notification
        )

        scheduleNextScan(0L)
    }

    private fun acquireWakeLock() {

        val powerManager =
            getSystemService(POWER_SERVICE) as PowerManager

        wakeLock =
            powerManager.newWakeLock(
                PowerManager.PARTIAL_WAKE_LOCK,
                "RevolutPumpHunter::ScannerWakeLock"
            )

        wakeLock?.setReferenceCounted(false)

        if (wakeLock?.isHeld != true) {
            wakeLock?.acquire()
        }
    }

    private fun releaseWakeLock() {

        try {

            if (wakeLock?.isHeld == true) {
                wakeLock?.release()
            }

        } catch (_: Exception) {
        }

        wakeLock = null
    }

    private fun scheduleNextScan(
        delayMs: Long
    ) {

        if (stopped) {
            return
        }

        scheduler.schedule(
            {
                performScan()
            },
            delayMs,
            TimeUnit.MILLISECONDS
        )
    }

    private fun performScan() {

        if (stopped) {
            return
        }

        try {

            val tickers =
                RevolutApi.getTickers()

            ScannerStatus.scanSuccessful(
                tickers.size
            )

            currentBackoffMs =
                NORMAL_SCAN_DELAY_MS

            for (ticker in tickers) {

                if (
                    PumpHistory.isTracking(
                        ticker.symbol
                    )
                ) {

                    PumpHistory.addPrice(
                        symbol = ticker.symbol,
                        price = ticker.lastPrice
                    )
                }

                val exitSignal =
                    ExitEngine.updatePrice(
                        symbol = ticker.symbol,
                        price = ticker.lastPrice
                    )

                if (exitSignal != null) {
                    handleExitSignal(
                        exitSignal
                    )
                }

                val pumpSignal =
                    PumpEngine.addPrice(
                        symbol = ticker.symbol,
                        price = ticker.lastPrice
                    )

                if (pumpSignal != null) {

                    ExitEngine.registerPump(
                        pumpSignal
                    )

                    PumpHistory.startTracking(
                        symbol = pumpSignal.symbol,
                        price = pumpSignal.currentPrice
                    )

                    handlePumpSignal(
                        pumpSignal
                    )
                }
            }

            scheduleNextScan(
                NORMAL_SCAN_DELAY_MS
            )

        } catch (
            e: RevolutRateLimitException
        ) {

            ScannerStatus.scanFailed(e)

            val serverWait =
                e.retryAfterMs

            val calculatedBackoff =
                maxOf(
                    serverWait + 5_000L,
                    currentBackoffMs * 2
                )

            currentBackoffMs =
                min(
                    calculatedBackoff,
                    MAX_BACKOFF_MS
                )

            ScannerStatus.startBackoff(
                currentBackoffMs
            )

            scheduleNextScan(
                currentBackoffMs
            )

        } catch (
            e: Exception
        ) {

            ScannerStatus.scanFailed(e)

            currentBackoffMs =
                min(
                    currentBackoffMs * 2,
                    MAX_BACKOFF_MS
                )

            scheduleNextScan(
                currentBackoffMs
            )
        }
    }

    private fun handlePumpSignal(
        signal: PumpSignal
    ) {

        val now =
            System.currentTimeMillis()

        val lastAlert =
            lastPumpAlertTime[
                signal.symbol
            ] ?: 0L

        val cooldown =
            15 * 60 * 1000L

        if (
            now - lastAlert <
            cooldown
        ) {
            return
        }

        lastPumpAlertTime[
            signal.symbol
        ] = now

        val notification =
            NotificationCompat.Builder(
                this,
                ALERT_CHANNEL_ID
            )
                .setSmallIcon(
                    android.R.drawable.ic_dialog_info
                )
                .setContentTitle(
                    "🚀 PUMP: ${signal.symbol}"
                )
                .setContentText(
                    "+${"%.2f".format(signal.changePercent)}% / " +
                        "${signal.durationMinutes} min | " +
                        "Pump Score ${signal.pumpScore}/10"
                )
                .setPriority(
                    NotificationCompat.PRIORITY_HIGH
                )
                .setAutoCancel(true)
                .build()

        val manager =
            getSystemService(
                NotificationManager::class.java
            )

        manager.notify(
            signal.symbol.hashCode(),
            notification
        )
    }

    private fun handleExitSignal(
        signal: ExitSignal
    ) {

        if (
            signal.exitScore < 50
        ) {
            return
        }

        val now =
            System.currentTimeMillis()

        val lastAlert =
            lastExitAlertTime[
                signal.symbol
            ] ?: 0L

        val cooldown =
            10 * 60 * 1000L

        if (
            now - lastAlert <
            cooldown
        ) {
            return
        }

        lastExitAlertTime[
            signal.symbol
        ] = now

        val notification =
            NotificationCompat.Builder(
                this,
                EXIT_CHANNEL_ID
            )
                .setSmallIcon(
                    android.R.drawable.ic_dialog_alert
                )
                .setContentTitle(
                    "🚪 ${signal.level}: ${signal.symbol}"
                )
                .setContentText(
                    "Exit Score ${signal.exitScore}/100 | " +
                        "od szczytu -${"%.2f".format(signal.dropFromPeakPercent)}% | " +
                        "momentum ${"%.2f".format(signal.momentumPercent)}%"
                )
                .setStyle(
                    NotificationCompat.BigTextStyle()
                        .bigText(
                            "${signal.symbol}\n" +
                                "Exit Score: ${signal.exitScore}/100\n" +
                                "Szczyt: ${"%.6f".format(signal.peakPrice)}\n" +
                                "Cena: ${"%.6f".format(signal.currentPrice)}\n" +
                                "Cofnięcie: -${"%.2f".format(signal.dropFromPeakPercent)}%\n" +
                                "Momentum: ${"%.2f".format(signal.momentumPercent)}%\n" +
                                "Status: ${signal.level}"
                        )
                )
                .setPriority(
                    NotificationCompat.PRIORITY_HIGH
                )
                .setAutoCancel(true)
                .build()

        val manager =
            getSystemService(
                NotificationManager::class.java
            )

        manager.notify(
            signal.symbol.hashCode() + 500000,
            notification
        )
    }

    private fun createNotificationChannels() {

        if (
            Build.VERSION.SDK_INT >=
            Build.VERSION_CODES.O
        ) {

            val scannerChannel =
                NotificationChannel(
                    CHANNEL_ID,
                    "Pump Hunter Scanner",
                    NotificationManager.IMPORTANCE_LOW
                )

            val pumpChannel =
                NotificationChannel(
                    ALERT_CHANNEL_ID,
                    "Pump Alerts",
                    NotificationManager.IMPORTANCE_HIGH
                )

            val exitChannel =
                NotificationChannel(
                    EXIT_CHANNEL_ID,
                    "Exit Signals",
                    NotificationManager.IMPORTANCE_HIGH
                )

            val manager =
                getSystemService(
                    NotificationManager::class.java
                )

            manager.createNotificationChannel(
                scannerChannel
            )

            manager.createNotificationChannel(
                pumpChannel
            )

            manager.createNotificationChannel(
                exitChannel
            )
        }
    }

    override fun onTaskRemoved(
        rootIntent: Intent?
    ) {

        /*
         * Gdy aplikacja zostanie usunięta z ostatnich,
         * nie zatrzymujemy ręcznie skanera.
         */
        super.onTaskRemoved(rootIntent)
    }

    override fun onDestroy() {

        stopped = true

        ScannerStatus.scannerStopped()

        scheduler.shutdownNow()

        releaseWakeLock()

        super.onDestroy()
    }

    override fun onStartCommand(
        intent: Intent?,
        flags: Int,
        startId: Int
    ): Int {

        return START_STICKY
    }

    override fun onBind(
        intent: Intent?
    ): IBinder? {

        return null
    }
}
