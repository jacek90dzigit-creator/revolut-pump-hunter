package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.ui.components.LiveSignalCard
import com.revolutscanner.ui.components.SectionHeader

@Composable
fun WatchlistScreen(
    favoriteAssets: Set<String>,
    signals: List<LiveSignalUi>,
    onFavorite: (String) -> Unit,
    onSignalClick: (LiveSignalUi) -> Unit
) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            SectionHeader(
                "★ Obserwowane",
                "Twoja trwała watchlista — gwiazdki nie znikają po zamknięciu aplikacji"
            )
        }

        if (favoriteAssets.isEmpty()) {
            item {
                Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)) {
                    Text(
                        "Nie masz jeszcze obserwowanych aktywów. Kliknij ☆ przy dowolnym sygnale.",
                        Modifier.padding(16.dp)
                    )
                }
            }
        } else {
            val missing = favoriteAssets - signals.map { it.asset }.toSet()

            items(signals, key = { it.id }) { signal ->
                LiveSignalCard(
                    signal = signal,
                    favorite = true,
                    onFavorite = { onFavorite(signal.asset) },
                    onClick = { onSignalClick(signal) }
                )
            }

            if (missing.isNotEmpty()) {
                item {
                    Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)) {
                        Column(Modifier.padding(14.dp)) {
                            Text("Obserwowane bez aktualnego sygnału", style = MaterialTheme.typography.titleSmall)
                            Spacer(Modifier.height(6.dp))
                            Text(
                                missing.sorted().joinToString(" • "),
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                    }
                }
            }
        }

        item { Spacer(Modifier.height(24.dp)) }
    }
}
