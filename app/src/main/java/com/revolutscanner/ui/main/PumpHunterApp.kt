package com.revolutscanner.ui.main

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.ui.screens.*

private enum class MainTab(val label: String, val icon: String) {
    HOME("Home", "⌂"),
    SIGNALS("Sygnały", "↗"),
    ACTIVE("Aktywne", "▲"),
    WATCHLIST("Obserwowane", "★"),
    SETTINGS("Ustawienia", "⚙")
}

@Composable
fun PumpHunterApp(
    mainViewModel: MainViewModel = viewModel()
) {
    var selectedTab by remember { mutableStateOf(MainTab.HOME) }
    var selectedSignalId by remember { mutableStateOf<String?>(null) }
    var shadowOpen by remember { mutableStateOf(false) }

    val selectedSignal = selectedSignalId?.let { id ->
        mainViewModel.signals.firstOrNull { it.id == id }
    }

    val strongest = mainViewModel.activeSignals.maxByOrNull {
        it.primaryScore ?: Int.MIN_VALUE
    }

    fun openSignal(signal: LiveSignalUi) {
        mainViewModel.ensurePriceContext(signal.asset)
        selectedSignalId = signal.id
    }

    Scaffold(
        modifier = Modifier.fillMaxSize(),
        containerColor = MaterialTheme.colorScheme.background,
        bottomBar = {
            if (selectedSignalId == null && !shadowOpen) {
                NavigationBar(
                    containerColor = MaterialTheme.colorScheme.surface
                ) {
                    MainTab.entries.forEach { tab ->
                        NavigationBarItem(
                            selected = selectedTab == tab,
                            onClick = { selectedTab = tab },
                            icon = {
                                Text(
                                    tab.icon,
                                    fontSize = 19.sp,
                                    fontWeight = FontWeight.Bold
                                )
                            },
                            label = {
                                Text(
                                    tab.label,
                                    fontSize = if (tab == MainTab.WATCHLIST) 9.sp else 10.sp
                                )
                            }
                        )
                    }
                }
            }
        }
    ) { padding ->
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(padding)
                .padding(top = 12.dp)
        ) {
            when {
                shadowOpen -> {
                    ShadowScreen(
                        shadow = mainViewModel.shadow,
                        loading = mainViewModel.shadowLoading,
                        error = mainViewModel.shadowError,
                        onBack = { shadowOpen = false },
                        onRefresh = { mainViewModel.refreshShadow(force = true) }
                    )
                }

                selectedSignal != null -> {
                    SignalDetailScreen(
                        signal = selectedSignal,
                        favorite = mainViewModel.isFavorite(selectedSignal.asset),
                        onFavorite = { mainViewModel.toggleFavorite(selectedSignal.asset) },
                        onBack = { selectedSignalId = null }
                    )
                }

                else -> {
                    when (selectedTab) {
                        MainTab.HOME -> DashboardScreen(
                            engineVersion = mainViewModel.server.version,
                            engineName = mainViewModel.server.engine,
                            online = mainViewModel.serverOnline,
                            loading = mainViewModel.isLoading,
                            error = mainViewModel.errorMessage,
                            trackedAssets = mainViewModel.trackedAssets,
                            pumpCount = mainViewModel.pumpCount,
                            exitCount = mainViewModel.exitCount,
                            watchlistCount = mainViewModel.favorites.size,
                            strongest = strongest,
                            shadow = mainViewModel.shadow,
                            shadowLoading = mainViewModel.shadowLoading,
                            shadowError = mainViewModel.shadowError,
                            isFavorite = mainViewModel::isFavorite,
                            onFavorite = mainViewModel::toggleFavorite,
                            onSignalClick = ::openSignal,
                            onOpenShadow = {
                                mainViewModel.refreshShadow(force = true)
                                shadowOpen = true
                            },
                            onRefresh = mainViewModel::refresh,
                            onRefreshShadow = { mainViewModel.refreshShadow(force = true) }
                        )

                        MainTab.SIGNALS -> SignalsScreen(
                            signals = mainViewModel.signals,
                            isFavorite = mainViewModel::isFavorite,
                            onFavorite = mainViewModel::toggleFavorite,
                            onSignalClick = ::openSignal
                        )

                        MainTab.ACTIVE -> ActiveScreen(
                            signals = mainViewModel.activeSignals,
                            isFavorite = mainViewModel::isFavorite,
                            onFavorite = mainViewModel::toggleFavorite,
                            onSignalClick = ::openSignal
                        )

                        MainTab.WATCHLIST -> WatchlistScreen(
                            favoriteAssets = mainViewModel.favorites,
                            signals = mainViewModel.watchlistSignals,
                            onFavorite = mainViewModel::toggleFavorite,
                            onSignalClick = ::openSignal
                        )

                        MainTab.SETTINGS -> SettingsScreen(
                            serverUrl = mainViewModel.serverUrl,
                            serverOnline = mainViewModel.serverOnline,
                            engineVersion = mainViewModel.server.version,
                            errorMessage = mainViewModel.errorMessage,
                            onSaveServerUrl = mainViewModel::saveServerUrl,
                            onRefresh = mainViewModel::refresh
                        )
                    }
                }
            }
        }
    }
}
