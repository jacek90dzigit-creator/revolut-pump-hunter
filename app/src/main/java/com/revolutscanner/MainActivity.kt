package com.revolutscanner

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.view.Gravity
import android.widget.Button
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat

class MainActivity : AppCompatActivity() {

    private lateinit var contentBox: LinearLayout

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        requestNotificationPermission()
        startScannerService()

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(24, 36, 24, 24)
        }

        val title = TextView(this).apply {
            text = "🔥 Revolut Pump Hunter"
            textSize = 26f
            gravity = Gravity.CENTER_HORIZONTAL
            setPadding(0, 0, 0, 24)
        }

        val tabs = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER
        }

        val pumpButton = Button(this).apply {
            text = "🔥 PUMP"
        }

        val activeButton = Button(this).apply {
            text = "📈 AKTYWNE"
        }

        val exitButton = Button(this).apply {
            text = "🚪 EXIT"
        }

        tabs.addView(
            pumpButton,
            LinearLayout.LayoutParams(
                0,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                1f
            )
        )

        tabs.addView(
            activeButton,
            LinearLayout.LayoutParams(
                0,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                1f
            )
        )

        tabs.addView(
            exitButton,
            LinearLayout.LayoutParams(
                0,
                LinearLayout.LayoutParams.WRAP_CONTENT,
                1f
            )
        )

        contentBox = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(8, 24, 8, 24)
        }

        val scrollView = ScrollView(this).apply {
            addView(contentBox)
        }

        root.addView(title)
        root.addView(tabs)
        root.addView(
            scrollView,
            LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f
            )
        )

        setContentView(root)

        pumpButton.setOnClickListener {
            showPumpTab()
        }

        activeButton.setOnClickListener {
            showActiveTab()
        }

        exitButton.setOnClickListener {
            showExitTab()
        }

        showPumpTab()
    }

    private fun showPumpTab() {
        contentBox.removeAllViews()

        addSectionTitle("🔥 PUMP HUNTER")

        addText(
            "Scanner działa w tle i obserwuje rynek Revolut X."
        )

        addText(
            "Domyślny sygnał: wzrost +5% w ciągu 30 minut."
        )

        addText(
            "Po wykryciu ruchu token automatycznie trafia do zakładki AKTYWNE."
        )
    }

    private fun showActiveTab() {
        contentBox.removeAllViews()

        addSectionTitle("📈 AKTYWNE PUMPY")

        val pumps = ExitEngine.getActivePumps()

        if (pumps.isEmpty()) {
            addText("Brak aktywnych pumpów.")
            return
        }

        for (pump in pumps) {

            val gainFromDetection =
                if (pump.detectedPrice > 0) {
                    ((pump.currentPrice - pump.detectedPrice) /
                        pump.detectedPrice) * 100.0
                } else {
                    0.0
                }

            val dropFromPeak =
                if (pump.peakPrice > 0) {
                    ((pump.peakPrice - pump.currentPrice) /
                        pump.peakPrice) * 100.0
                } else {
                    0.0
                }

            addText(
                "${pump.symbol}\n" +
                    "Cena wykrycia: ${formatPrice(pump.detectedPrice)}\n" +
                    "Aktualna: ${formatPrice(pump.currentPrice)}\n" +
                    "Szczyt: ${formatPrice(pump.peakPrice)}\n" +
                    "Od wykrycia: ${formatPercent(gainFromDetection)}\n" +
                    "Od szczytu: -${"%.2f".format(dropFromPeak)}%\n" +
                    "Exit Score: ${pump.exitScore}/100\n" +
                    "Status: ${pump.status}"
            )

            val history =
                PumpHistory.getHistory(pump.symbol)

            val chart =
                PumpChartView(this).apply {
                    setHistory(history)
                }

            contentBox.addView(
                chart,
                LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    600
                ).apply {
                    setMargins(0, 8, 0, 32)
                }
            )
        }
    }

    private fun showExitTab() {
        contentBox.removeAllViews()

        addSectionTitle("🚪 EXIT SIGNAL")

        val pumps = ExitEngine.getActivePumps()
            .filter { it.exitScore >= 50 }
            .sortedByDescending { it.exitScore }

        if (pumps.isEmpty()) {
            addText("Brak aktywnych sygnałów wyjścia.")
            return
        }

        for (pump in pumps) {

            addText(
                "${pump.symbol}\n" +
                    "Exit Score: ${pump.exitScore}/100\n" +
                    "Aktualna cena: ${formatPrice(pump.currentPrice)}\n" +
                    "Szczyt: ${formatPrice(pump.peakPrice)}\n" +
                    "Status: ${pump.status}"
            )

            val history =
                PumpHistory.getHistory(pump.symbol)

            val chart =
                PumpChartView(this).apply {
                    setHistory(history)
                }

            contentBox.addView(
                chart,
                LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    500
                ).apply {
                    setMargins(0, 8, 0, 32)
                }
            )
        }
    }

    private fun addSectionTitle(text: String) {
        val view = TextView(this).apply {
            this.text = text
            textSize = 22f
            setPadding(0, 8, 0, 20)
        }

        contentBox.addView(view)
    }

    private fun addText(text: String) {
        val view = TextView(this).apply {
            this.text = text
            textSize = 17f
            setPadding(12, 12, 12, 24)
        }

        contentBox.addView(view)
    }

    private fun formatPrice(price: Double): String {
        return when {
            price >= 1000 -> "%.2f".format(price)
            price >= 1 -> "%.4f".format(price)
            else -> "%.8f".format(price)
        }
    }

    private fun formatPercent(value: Double): String {
        return if (value >= 0) {
            "+${"%.2f".format(value)}%"
        } else {
            "${"%.2f".format(value)}%"
        }
    }

    private fun startScannerService() {
        val serviceIntent =
            Intent(this, ScannerService::class.java)

        ContextCompat.startForegroundService(
            this,
            serviceIntent
        )
    }

    private fun requestNotificationPermission() {

        if (Build.VERSION.SDK_INT >=
            Build.VERSION_CODES.TIRAMISU
        ) {

            if (
                ContextCompat.checkSelfPermission(
                    this,
                    Manifest.permission.POST_NOTIFICATIONS
                ) != PackageManager.PERMISSION_GRANTED
            ) {

                ActivityCompat.requestPermissions(
                    this,
                    arrayOf(
                        Manifest.permission.POST_NOTIFICATIONS
                    ),
                    1001
                )
            }
        }
    }
}
