package com.revolutscanner

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.widget.LinearLayout
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat

class MainActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        requestNotificationPermission()

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(32, 48, 32, 32)
        }

        val title = TextView(this).apply {
            text = "🔥 Revolut Pump Hunter"
            textSize = 26f
        }

        val subtitle = TextView(this).apply {
            text = "Pump Hunter • Aktywne Pumpy • Exit Signal"
            textSize = 16f
            setPadding(0, 16, 0, 24)
        }

        val pump = TextView(this).apply {
            text = "🔥 PUMP HUNTER\nSkanowanie rynku..."
            textSize = 20f
            setPadding(0, 16, 0, 24)
        }

        val active = TextView(this).apply {
            text = "📈 AKTYWNE PUMPY\nBrak aktywnych sygnałów"
            textSize = 20f
            setPadding(0, 16, 0, 24)
        }

        val exit = TextView(this).apply {
            text = "🚪 EXIT SIGNAL\nBrak sygnałów wyjścia"
            textSize = 20f
            setPadding(0, 16, 0, 24)
        }

        root.addView(title)
        root.addView(subtitle)
        root.addView(pump)
        root.addView(active)
        root.addView(exit)

        setContentView(root)

        val serviceIntent = Intent(this, ScannerService::class.java)
        ContextCompat.startForegroundService(this, serviceIntent)
    }

    private fun requestNotificationPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(
                    this,
                    Manifest.permission.POST_NOTIFICATIONS
                ) != PackageManager.PERMISSION_GRANTED
            ) {
                ActivityCompat.requestPermissions(
                    this,
                    arrayOf(Manifest.permission.POST_NOTIFICATIONS),
                    1001
                )
            }
        }
    }
}
