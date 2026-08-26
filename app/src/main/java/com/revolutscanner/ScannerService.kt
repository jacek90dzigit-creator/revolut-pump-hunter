package com.revolutscanner

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ScannerService : Service() {

    companion object {
        const val CHANNEL_ID = "pump_hunter_scanner"
        const val ALERT_CHANNEL_ID = "pump_hunter_alerts"
        const val EXIT_CHANNEL_ID = "pump_hunter_exit"
        const val NOTIFICATION_ID = 101
    }

    private val scheduler =
        Executors.newSingleThreadScheduledExecutor()

    private val lastPumpAlertTime =
        mutableMapOf<String, Long>()

    private val lastExitAlertTime =
        mutableMapOf<String, Long>()

    override fun onCreate() {
        super.onCreate()

        ScannerStatus.scannerStarted()

        createNotificationChannels()

        val notification: Notification =
            NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("🔥 Revolut Pump Hunter")
                .setContentText("Scanner rynku działa w tle")
                .setSmallIcon(android.R.drawable.ic_popup_sync)
                .setOngoing(true)
                .build()

        startForeground(
            NOTIFICATION_ID,
            notification
        )

        startScanner()
    }

    private fun startScanner() {

        scheduler.scheduleAtFixedRate({

            try {

                val tickers =
                    RevolutApi.getTickers()

                ScannerStatus.scanSuccessful(
                    tickers.size
                )

                for (ticker in tickers) {

                    if (PumpHistory.isTracking(ticker.symbol)) {
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
                        handleExitSignal(exitSignal)
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

            } catch (e: Exception) {

                ScannerStatus.scanFailed(e)

                e.printStackTrace()
            }

        }, 0, 60, TimeUnit.SECONDS)
    }

    private fun handlePumpSignal(
        signal: PumpSignal
    ) {

        val now =
            System.currentTimeMillis()

        val lastAlert =
            lastPumpAlertTime[signal.symbol] ?: 0L

        val cooldown =
            15 * 60 * 1000L

        if (now - lastAlert < cooldown) {
            return
        }

        lastPumpAlertTime[signal.symbol] = now

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

        if (signal.exitScore < 50) {
            return
        }

        val now =
            System.currentTimeMillis()

        val lastAlert =
            lastExitAlertTime[signal.symbol] ?: 0L

        val cooldown =
            10 * 60 * 1000L

        if (now - lastAlert < cooldown) {
            return
        }

        lastExitAlertTime[signal.symbol] = now

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

        if (Build.VERSION.SDK_INT >=
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

            manager.createNotificationChannel(scannerChannel)
            manager.createNotificationChannel(pumpChannel)
            manager.createNotificationChannel(exitChannel)
        }
    }

    override fun onDestroy() {
        ScannerStatus.scannerStopped()
        scheduler.shutdownNow()
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
