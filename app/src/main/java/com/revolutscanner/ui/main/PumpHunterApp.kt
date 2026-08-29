package com.revolutscanner.ui.main

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import com.revolutscanner.ui.screens.ActiveScreen
import com.revolutscanner.ui.screens.DashboardScreen
import com.revolutscanner.ui.screens.HistoryScreen
import com.revolutscanner.ui.screens.SettingsScreen
import com.revolutscanner.ui.screens.SignalsScreen

private enum class MainTab(
    val label: String,
    val icon: String
) {
    HOME("Home", "⌂"),
    SIGNALS("Sygnały", "↗"),
    ACTIVE("Aktywne", "▲"),
    HISTORY("Historia", "◷"),
    SETTINGS("Ustawienia", "⚙")
}

@Composable
fun PumpHunterApp(
    mainViewModel: MainViewModel = viewModel()
) {
    var selectedTab by remember {
        mutableStateOf(MainTab.HOME)
    }

    Scaffold(
        modifier = Modifier.fillMaxSize(),
        containerColor = MaterialTheme.colorScheme.background,
        bottomBar = {
            NavigationBar(
                containerColor = MaterialTheme.colorScheme.surface
            ) {
                MainTab.entries.forEach { tab ->
                    NavigationBarItem(
                        selected = selectedTab == tab,
                        onClick = { selectedTab = tab },
                        icon = {
                            Text(
                                text = tab.icon,
                                fontSize = 20.sp,
                                fontWeight = FontWeight.Bold
                            )
                        },
                        label = {
                            Text(
                                text = tab.label,
                                fontSize = 10.sp
                            )
                        }
                    )
                }
            }
        }
    ) { padding ->
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(padding)
                .padding(top = 14.dp)
        ) {
            when (selectedTab) {
                MainTab.HOME -> DashboardScreen(
                    stats = mainViewModel.dashboard,
                    strongestPump = mainViewModel.activePumps.maxByOrNull { it.pumpScore }
                )

                MainTab.SIGNALS -> SignalsScreen(
                    signals = mainViewModel.signals
                )

                MainTab.ACTIVE -> ActiveScreen(
                    pumps = mainViewModel.activePumps
                )

                MainTab.HISTORY -> HistoryScreen(
                    history = mainViewModel.history
                )

                MainTab.SETTINGS -> SettingsScreen()
            }
        }
    }
}
