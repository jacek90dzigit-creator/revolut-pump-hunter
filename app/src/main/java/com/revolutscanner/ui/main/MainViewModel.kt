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
import com.revolutscanner.domain.model.ServerStatusUi
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

class MainViewModel(application: Application) : AndroidViewModel(application) {

    private val preferences = AppPreferences(application)
    private val api = PumpHunterLiveApi { preferences.serverUrl }
    private val repository = LivePumpHunterRepository(api)
    private val executor = Executors.newSingleThreadExecutor()
    private val mainHandler = Handler(Looper.getMainLooper())
    private val refreshing = AtomicBoolean(false)

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
                    signals = snapshot.signals
                    trackedAssets = snapshot.trackedAssets
                    lastUpdateMillis = snapshot.lastUpdateMillis
                    errorMessage = null
                    isLoading = false
                    refreshing.set(false)
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

    fun saveServerUrl(value: String) {
        preferences.serverUrl = value
        serverUrl = preferences.serverUrl
        errorMessage = null
        refresh()
    }

    fun toggleFavorite(asset: String) {
        favorites = preferences.toggleFavorite(asset)
    }

    fun isFavorite(asset: String): Boolean = favorites.contains(asset.uppercase())

    override fun onCleared() {
        mainHandler.removeCallbacks(refreshLoop)
        executor.shutdownNow()
        super.onCleared()
    }

    companion object {
        private const val REFRESH_INTERVAL_MS = 15_000L
        private val ACTIVE_TYPES = setOf("EARLY_MOVE", "PUMP", "COOLING", "RE_ENTRY")
    }
}
