package com.revolutscanner

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat

class ScannerService : Service() {

    companion object {
        const val CHANNEL_ID = "pump_hunter_scanner"
        const val NOTIFICATION_ID = 101
    }

    override fun onCreate() {
        super.onCreate()

        createNotificationChannel()

        val notification: Notification =
            NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("🔥 Revolut Pump Hunter")
                .setContentText("Scanner rynku działa w tle")
                .setSmallIcon(android.R.drawable.ic_popup_sync)
                .setOngoing(true)
                .build()

        startForeground(NOTIFICATION_ID, notification)
    }

    override fun onStartCommand(
        intent: Intent?,
        flags: Int,
        startId: Int
    ): Int {

        /*
         * Tutaj za chwilę dodamy właściwy silnik:
         *
         * Revolut X API
         *       ↓
         * ceny + OHLCV
         *       ↓
         * Pump Score
         *       ↓
         * aktywny pump
         *       ↓
         * monitoring szczytu
         *       ↓
         * Exit Score
         */

        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? {
        return null
    }

    private fun createNotificationChannel() {

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {

            val channel = NotificationChannel(
                CHANNEL_ID,
                "Pump Hunter Scanner",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Skanowanie rynku Revolut w tle"
            }

            val manager =
                getSystemService(NotificationManager::class.java)

            manager.createNotificationChannel(channel)
        }
    }
}
