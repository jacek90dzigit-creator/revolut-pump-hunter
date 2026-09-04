package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.domain.model.ShadowSnapshotUi
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
    shadow: ShadowSnapshotUi?,
    shadowLoading: Boolean,
    shadowError: String?,
    isFavorite: (String) -> Boolean,
    onFavorite: (String) -> Unit,
    onSignalClick: (LiveSignalUi) -> Unit,
    onOpenShadow: () -> Unit,
    onRefresh: () -> Unit,
    onRefreshShadow: () -> Unit
) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            SectionHeader(
                title = "PUMP HUNTER",
                subtitle = "Android 3.2  •  Engine $engineVersion  •  $engineName"
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
            ShadowHomeCard(
                shadow = shadow,
                loading = shadowLoading,
                error = shadowError,
                onOpen = onOpenShadow,
                onRefresh = onRefreshShadow
            )
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

@Composable
private fun ShadowHomeCard(
    shadow: ShadowSnapshotUi?,
    loading: Boolean,
    error: String?,
    onOpen: () -> Unit,
    onRefresh: () -> Unit
) {
    Card(
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.surfaceVariant
        )
    ) {
        Column(Modifier.padding(16.dp)) {
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Column {
                    Text(
                        "🧪 ${shadow?.candidateName ?: "Candidate #5G"}",
                        fontWeight = FontWeight.Bold,
                        style = MaterialTheme.typography.titleMedium
                    )
                    Text(
                        "SHADOW • P21 + P22.1 • forward-OOS",
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        style = MaterialTheme.typography.bodySmall
                    )
                }
                if (shadow?.shadowOnly != false) {
                    AssistChip(
                        onClick = {},
                        label = { Text("SHADOW") }
                    )
                }
            }

            Spacer(Modifier.height(12.dp))

            if (shadow != null) {
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    StatCard(shadow.candidateEvents.toString(), "OOS", Modifier.weight(1f))
                    StatCard(shadow.comparisonsComplete.toString(), "porównane", Modifier.weight(1f))
                    StatCard(shadow.comparisonsPending.toString(), "czekają", Modifier.weight(1f))
                }

                Spacer(Modifier.height(10.dp))

                val lead = shadow.averageCandidateLeadMinutes?.let {
                    String.format("%+.1f min", it)
                } ?: "—"

                Text(
                    "Candidate wcześniej: ${shadow.candidateBeforeEngine}  •  " +
                        "Engine wcześniej: ${shadow.engineBeforeCandidate}",
                    style = MaterialTheme.typography.bodySmall
                )
                Text(
                    "Średni lead Candidate: $lead",
                    style = MaterialTheme.typography.bodySmall,
                    fontWeight = FontWeight.SemiBold
                )

                Spacer(Modifier.height(10.dp))

                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(onClick = onOpen, modifier = Modifier.weight(1f)) {
                        Text("Otwórz Shadow")
                    }
                    OutlinedButton(onClick = onRefresh) {
                        Text(if (loading) "…" else "↻")
                    }
                }
            } else {
                if (error != null) {
                    Text(
                        "Shadow API: $error",
                        color = MaterialTheme.colorScheme.error,
                        style = MaterialTheme.typography.bodySmall
                    )
                } else {
                    Text(
                        if (loading) "Pobieram dane shadow…" else "Brak danych shadow.",
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                Spacer(Modifier.height(8.dp))
                OutlinedButton(onClick = onRefresh) { Text("Odśwież Shadow") }
            }
        }
    }
}
