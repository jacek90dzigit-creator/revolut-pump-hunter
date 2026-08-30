package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.ui.components.*

@Composable
fun DashboardScreen(
    engineVersion: String,
    engineName: String,
    online: Boolean,
    loading: Boolean,
    error: String?,
    trackedAssets: Int,
    pumpCount: Int,
    exitCount: Int,
    watchlistCount: Int,
    strongest: LiveSignalUi?,
    isFavorite: (String) -> Boolean,
    onFavorite: (String) -> Unit,
    onSignalClick: (LiveSignalUi) -> Unit,
    onRefresh: () -> Unit
) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            SectionHeader(
                title = "PUMP HUNTER",
                subtitle = "Android 3.1  •  Engine $engineVersion  •  $engineName"
            )
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                ServerStatusBadge(online)
                if (loading) {
                    Text("odświeżanie…", color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
            }
        }

        if (error != null) {
            item {
                Card(
                    colors = CardDefaults.cardColors(
                        containerColor = MaterialTheme.colorScheme.error.copy(alpha = 0.12f)
                    )
                ) {
                    Column(Modifier.padding(14.dp)) {
                        Text("Problem z połączeniem", color = MaterialTheme.colorScheme.error)
                        Spacer(Modifier.height(4.dp))
                        Text(error, style = MaterialTheme.typography.bodySmall)
                        Spacer(Modifier.height(8.dp))
                        OutlinedButton(onClick = onRefresh) { Text("Spróbuj ponownie") }
                    }
                }
            }
        }

        item {
            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                StatCard(trackedAssets.toString(), "monitorowane", Modifier.weight(1f))
                StatCard(pumpCount.toString(), "PUMP / RE-ENTRY", Modifier.weight(1f))
            }
        }

        item {
            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                StatCard(exitCount.toString(), "EXIT", Modifier.weight(1f))
                StatCard(watchlistCount.toString(), "obserwowane ★", Modifier.weight(1f))
            }
        }

        item {
            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                Text("Najmocniejszy aktualny sygnał", style = MaterialTheme.typography.titleMedium)
                TextButton(onClick = onRefresh) { Text("Odśwież") }
            }
        }

        if (strongest == null) {
            item {
                Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)) {
                    Text(
                        "Brak aktywnych sygnałów do pokazania.",
                        modifier = Modifier.padding(16.dp),
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        } else {
            item {
                LiveSignalCard(
                    signal = strongest,
                    favorite = isFavorite(strongest.asset),
                    onFavorite = { onFavorite(strongest.asset) },
                    onClick = { onSignalClick(strongest) }
                )
            }
        }

        item { Spacer(Modifier.height(24.dp)) }
    }
}
