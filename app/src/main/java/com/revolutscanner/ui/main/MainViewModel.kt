package com.revolutscanner.ui.main

import android.app.Application
import android.os.Handler
import android.os.Looper
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.AndroidViewModel
import com.revolutscanner.data.api.PumpHunterLiveApi
import com.revolutscanner.data.local.AppPreferences
import com.revolutscanner.data.repository.LivePumpHunterRepository
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.domain.model.PriceContextUi
import com.revolutscanner.domain.model.ServerStatusUi
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

class MainViewModel(application: Application) : AndroidViewModel(application) {

    private val preferences = AppPreferences(application)
    private val api = PumpHunterLiveApi { preferences.serverUrl }
    private val repository = LivePumpHunterRepository(api)
    private val executor = Executors.newSingleThreadExecutor()
    private val contextExecutor = Executors.newFixedThreadPool(4)
    private val mainHandler = Handler(Looper.getMainLooper())
    private val refreshing = AtomicBoolean(false)

    private val contextLastAttemptMs = ConcurrentHashMap<String, Long>()
    private val contextInFlight = Collections.synchronizedSet(mutableSetOf<String>())

    var serverUrl by mutableStateOf(preferences.serverUrl)
        private set

    var server by mutableStateOf(ServerStatusUi())
        private set

    var signals by mutableStateOf<List<LiveSignalUi>>(emptyList())
        private set

    var trackedAssets by mutableStateOf(0)
        private set

    var lastUpdateMillis by mutableStateOf<Long?>(null)
        private set

    var isLoading by mutableStateOf(false)
        private set

    var errorMessage by mutableStateOf<String?>(null)
        private set

    var favorites by mutableStateOf(preferences.favorites())
        private set

    var priceContexts by mutableStateOf<Map<String, PriceContextUi>>(emptyMap())
        private set

    val serverOnline: Boolean
        get() = errorMessage == null && lastUpdateMillis != null

    val latestByAsset: List<LiveSignalUi>
        get() = signals
            .groupBy { it.asset }
            .mapNotNull { (_, items) -> items.maxByOrNull { it.timestamp } }
            .sortedByDescending { it.timestamp }

    val activeSignals: List<LiveSignalUi>
        get() = latestByAsset
            .filter { it.type in ACTIVE_TYPES }
            .sortedWith(
                compareByDescending<LiveSignalUi> { it.primaryScore ?: Int.MIN_VALUE }
                    .thenByDescending { it.timestamp }
            )

    val watchlistSignals: List<LiveSignalUi>
        get() = latestByAsset
            .filter { favorites.contains(it.asset) }
            .sortedByDescending { it.timestamp }

    val pumpCount: Int
        get() = latestByAsset.count { it.type == "PUMP" || it.type == "RE_ENTRY" }

    val exitCount: Int
        get() = latestByAsset.count { it.type == "EXIT" }

    private val refreshLoop = object : Runnable {
        override fun run() {
            refresh()
            mainHandler.postDelayed(this, REFRESH_INTERVAL_MS)
        }
    }

    init {
        refresh()
        mainHandler.postDelayed(refreshLoop, REFRESH_INTERVAL_MS)
    }

    fun refresh() {
        if (!refreshing.compareAndSet(false, true)) return

        if (preferences.serverUrl.isBlank()) {
            isLoading = false
            errorMessage = "Ustaw adres serwera Oracle w zakładce Ustawienia."
            refreshing.set(false)
            return
        }

        isLoading = true

        executor.execute {
            try {
                val snapshot = repository.refresh()
                mainHandler.post {
                    server = snapshot.server
                    signals = snapshot.signals.map { signal ->
                        signal.copy(priceContext = priceContexts[signal.asset])
                    }
                    trackedAssets = snapshot.trackedAssets
                    lastUpdateMillis = snapshot.lastUpdateMillis
                    errorMessage = null
                    isLoading = false
                    refreshing.set(false)

                    schedulePriceContexts(signals)
                }
            } catch (e: Exception) {
                mainHandler.post {
                    errorMessage = e.message ?: e.javaClass.simpleName
                    isLoading = false
                    refreshing.set(false)
                }
            }
        }
    }

    private fun schedulePriceContexts(currentSignals: List<LiveSignalUi>) {
        if (preferences.serverUrl.isBlank()) return

        val latestAssets = currentSignals
            .groupBy { it.asset }
            .mapNotNull { (_, items) -> items.maxByOrNull { it.timestamp } }
            .sortedByDescending { it.timestamp }
            .map { it.asset }

        val targets = (favorites.toList() + latestAssets)
            .distinct()
            .take(MAX_CONTEXT_ASSETS)

        targets.forEach { ensurePriceContext(it) }
    }

    fun ensurePriceContext(asset: String, force: Boolean = false) {
        val symbol = asset.uppercase()
        val now = System.currentTimeMillis()
        val lastAttempt = contextLastAttemptMs[symbol] ?: 0L

        if (!force && now - lastAttempt < PRICE_CONTEXT_TTL_MS) return
        if (!contextInFlight.add(symbol)) return

        contextLastAttemptMs[symbol] = now

        contextExecutor.execute {
            try {
                val context = api.fetchPriceContext(symbol)
                mainHandler.post {
                    priceContexts = priceContexts + (symbol to context)
                    signals = signals.map { signal ->
                        if (signal.asset == symbol) signal.copy(priceContext = context) else signal
                    }
                }
            } catch (_: Exception) {
                // Kontekst 1D/3D/5D jest wyłącznie informacyjny.
                // Jego błąd nie może przełączyć całej aplikacji w OFFLINE.
            } finally {
                contextInFlight.remove(symbol)
            }
        }
    }

    fun saveServerUrl(value: String) {
        preferences.serverUrl = value
        serverUrl = preferences.serverUrl
        errorMessage = null
        priceContexts = emptyMap()
        contextLastAttemptMs.clear()
        refresh()
    }

    fun toggleFavorite(asset: String) {
        favorites = preferences.toggleFavorite(asset)
        ensurePriceContext(asset)
    }

    fun isFavorite(asset: String): Boolean = favorites.contains(asset.uppercase())

    override fun onCleared() {
        mainHandler.removeCallbacks(refreshLoop)
        executor.shutdownNow()
        contextExecutor.shutdownNow()
        super.onCleared()
    }

    companion object {
        private const val REFRESH_INTERVAL_MS = 15_000L
        private const val PRICE_CONTEXT_TTL_MS = 10 * 60 * 1000L
        private const val MAX_CONTEXT_ASSETS = 24
        private val ACTIVE_TYPES = setOf("EARLY_MOVE", "PUMP", "COOLING", "RE_ENTRY")
    }
}
