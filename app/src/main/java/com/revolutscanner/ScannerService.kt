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
        const val NOTIFICATION_ID = 101
    }

    private val scheduler =
        Executors.newSingleThreadScheduledExecutor()

    private val lastAlertTime =
        mutableMapOf<String, Long>()

    override fun onCreate() {
        super.onCreate()

        createNotificationChannels()

        val notification: Notification =
            NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("🔥 Revolut Pump Hunter")
                .setContentText("Scanner rynku działa w tle")
                .setSmallIcon(android.R.drawable.ic_popup_sync)
                .setOngoing(true)
                .build()

        startForeground(NOTIFICATION_ID, notification)

        startScanner()
    }

    private fun startScanner() {

        scheduler.scheduleAtFixedRate({

            try {

                val tickers = RevolutApi.getTickers()

                for (ticker in tickers) {

                    val signal =
                        PumpEngine.addPrice(
                            symbol = ticker.symbol,
                            price = ticker.lastPrice
                        )

                    if (signal != null) {
                        handlePumpSignal(signal)
                    }
                }

            } catch (e: Exception) {
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
            lastAlertTime[signal.symbol] ?: 0L

        val cooldown =
            15 * 60 * 1000L

        if (now - lastAlert < cooldown) {
            return
        }

        lastAlertTime[signal.symbol] = now

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
                        "Score ${signal.pumpScore}/10"
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

            val alertChannel =
                NotificationChannel(
                    ALERT_CHANNEL_ID,
                    "Pump Hunter Alerts",
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
                alertChannel
            )
        }
    }

    override fun onDestroy() {
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
