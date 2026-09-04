package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.revolutscanner.domain.model.ShadowComparisonUi
import com.revolutscanner.domain.model.ShadowSnapshotUi
import java.util.Locale

@Composable
fun ShadowScreen(
    shadow: ShadowSnapshotUi?,
    loading: Boolean,
    error: String?,
    onBack: () -> Unit,
    onRefresh: () -> Unit
) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                OutlinedButton(onClick = onBack) { Text("← Wróć") }
                OutlinedButton(onClick = onRefresh) {
                    Text(if (loading) "Odświeżam…" else "↻ Odśwież")
                }
            }
        }

        item {
            Text(
                "🧪 Candidate #5G • SHADOW",
                fontSize = 26.sp,
                fontWeight = FontWeight.Bold
            )
            Text(
                "P21 forward-OOS + P22.1 comparator • tylko obserwacja",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 13.sp
            )
        }

        if (error != null) {
            item {
                Card(
                    colors = CardDefaults.cardColors(
                        containerColor = MaterialTheme.colorScheme.error.copy(alpha = 0.12f)
                    )
                ) {
                    Text(
                        "Shadow API: $error",
                        modifier = Modifier.padding(14.dp),
                        color = MaterialTheme.colorScheme.error
                    )
                }
            }
        }

        if (shadow != null) {
            item {
                ShadowSummary(shadow)
            }

            item {
                Text(
                    "Porównania Candidate ↔ Engine",
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.Bold
                )
            }

            items(shadow.comparisons, key = { it.eventId }) { item ->
                ShadowComparisonCard(item)
            }
        } else if (error == null) {
            item {
                Text(
                    if (loading) "Pobieram dane…" else "Brak danych shadow.",
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }

        item { Spacer(Modifier.height(24.dp)) }
    }
}

@Composable
private fun ShadowSummary(shadow: ShadowSnapshotUi) {
    Card(
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.surfaceVariant
        )
    ) {
        Column(Modifier.padding(16.dp)) {
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                MiniStat("P21", shadow.p21Status, Modifier.weight(1f))
                MiniStat("P22.1", shadow.p22Status, Modifier.weight(1f))
                MiniStat("OOS", shadow.candidateEvents.toString(), Modifier.weight(1f))
            }

            Spacer(Modifier.height(8.dp))

            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                MiniStat("Porównane", shadow.comparisonsComplete.toString(), Modifier.weight(1f))
                MiniStat("Czekają", shadow.comparisonsPending.toString(), Modifier.weight(1f))
                MiniStat(
                    "Lead avg",
                    shadow.averageCandidateLeadMinutes?.let { fmtMin(it) } ?: "—",
                    Modifier.weight(1f)
                )
            }

            Spacer(Modifier.height(10.dp))
            Text(
                "Candidate wcześniej: ${shadow.candidateBeforeEngine}  •  " +
                    "Engine wcześniej: ${shadow.engineBeforeCandidate}  •  " +
                    "Remis: ${shadow.sameTimestamp}",
                style = MaterialTheme.typography.bodySmall
            )
            shadow.updatedAt?.let {
                Spacer(Modifier.height(4.dp))
                Text(
                    "Aktualizacja: $it",
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    style = MaterialTheme.typography.bodySmall
                )
            }
        }
    }
}

@Composable
private fun MiniStat(label: String, value: String, modifier: Modifier = Modifier) {
    Surface(
        modifier = modifier,
        color = MaterialTheme.colorScheme.surface,
        shape = MaterialTheme.shapes.medium
    ) {
        Column(Modifier.padding(10.dp)) {
            Text(
                label,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 10.sp
            )
            Text(value, fontWeight = FontWeight.Bold, fontSize = 15.sp)
        }
    }
}

@Composable
private fun ShadowComparisonCard(item: ShadowComparisonUi) {
    val relationColor = when {
        item.relation.contains("CANDIDATE_BEFORE", true) ||
            item.relation.contains("ENGINE_AFTER_CANDIDATE", true) ->
            MaterialTheme.colorScheme.secondary

        item.relation.contains("ENGINE_BEFORE", true) ->
            MaterialTheme.colorScheme.tertiary

        !item.engineSignalFound ->
            MaterialTheme.colorScheme.primary

        else -> MaterialTheme.colorScheme.onSurfaceVariant
    }

    Card(
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.surfaceVariant
        )
    ) {
        Column(Modifier.padding(14.dp)) {
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Column {
                    Text(item.asset, fontSize = 23.sp, fontWeight = FontWeight.Bold)
                    Text(
                        item.symbol,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontSize = 11.sp
                    )
                }
                Surface(
                    color = relationColor.copy(alpha = 0.14f),
                    shape = MaterialTheme.shapes.large
                ) {
                    Text(
                        relationLabel(item),
                        modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp),
                        color = relationColor,
                        fontWeight = FontWeight.Bold,
                        fontSize = 10.sp
                    )
                }
            }

            Spacer(Modifier.height(8.dp))
            Text(
                item.candidateTimestampUtc ?: "czas Candidate: —",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 11.sp
            )

            Spacer(Modifier.height(10.dp))
            Text("CANDIDATE #5G", fontWeight = FontWeight.Bold, fontSize = 11.sp)

            MetricLine(
                "RVOL 15m / 1h",
                "${fmtX(item.rvol15m)}  /  ${fmtX(item.rvol1h)}"
            )
            MetricLine(
                "Streak / accel",
                "${item.rvolRisingStreak ?: "—"}  /  ${fmtX(item.rvolAccel)}"
            )
            MetricLine(
                "Compression / expansion",
                "${fmtNum(item.compressionRatio)}  /  ${fmtNum(item.rangeExpansion)}"
            )
            MetricLine(
                "Price 15m / 1h",
                "${fmtPct(item.price15mPct)}  /  ${fmtPct(item.price1hPct)}"
            )
            MetricLine("Taker buy ratio", fmtNum(item.takerBuyRatio))
            MetricLine("BTC 1h", fmtPct(item.btc1hPct))

            Spacer(Modifier.height(10.dp))
            HorizontalDivider()
            Spacer(Modifier.height(10.dp))

            Text("PRODUCTION ENGINE 3.2.0", fontWeight = FontWeight.Bold, fontSize = 11.sp)

            if (!item.engineSignalFound) {
                Text(
                    "🧪 CANDIDATE ONLY • czekamy na Engine",
                    color = MaterialTheme.colorScheme.primary,
                    fontWeight = FontWeight.Bold,
                    fontSize = 13.sp
                )
            } else {
                MetricLine("Sygnał", item.engineSignalType ?: "—")
                MetricLine(
                    "Lead Candidate",
                    item.leadMinutes?.let { fmtMin(it) } ?: "—",
                    valueColor = leadColor(item.leadMinutes)
                )
                MetricLine(
                    "Quality / Fusion",
                    "${item.engineQualityScore ?: "—"} / " +
                        "${item.fusionScore ?: "—"}${item.fusionGrade?.let { "/$it" } ?: ""}"
                )
                MetricLine("Entry", item.engineEntryStatus ?: "—")
                MetricLine("Move stage", item.engineMoveStage ?: "—")
                MetricLine(
                    "Fusion allowed",
                    item.fusionAllowed?.let { if (it) "TAK" else "NIE" } ?: "—"
                )
                item.fusionReason?.let { MetricLine("Decision", it) }

                Spacer(Modifier.height(10.dp))
                Text("OUTCOME PRODUKCYJNEGO SYGNAŁU", fontWeight = FontWeight.Bold, fontSize = 11.sp)
                Spacer(Modifier.height(5.dp))

                Row(
                    Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    item.outcomes.forEach { outcome ->
                        Surface(
                            modifier = Modifier.weight(1f),
                            color = MaterialTheme.colorScheme.surface,
                            shape = MaterialTheme.shapes.small
                        ) {
                            Column(Modifier.padding(7.dp)) {
                                Text(
                                    "${outcome.horizonMinutes}m",
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                    fontSize = 9.sp
                                )
                                Text(
                                    fmtPct(outcome.changePct),
                                    fontWeight = FontWeight.Bold,
                                    fontSize = 11.sp
                                )
                                Text(
                                    "↑${fmtPctPlain(outcome.maxGainPct)}",
                                    color = MaterialTheme.colorScheme.secondary,
                                    fontSize = 9.sp
                                )
                                Text(
                                    "↓${fmtPctPlain(outcome.maxDrawdownPct)}",
                                    color = MaterialTheme.colorScheme.error,
                                    fontSize = 9.sp
                                )
                            }
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun MetricLine(
    label: String,
    value: String,
    valueColor: Color = MaterialTheme.colorScheme.onSurface
) {
    Row(
        Modifier.fillMaxWidth().padding(vertical = 2.dp),
        horizontalArrangement = Arrangement.SpaceBetween
    ) {
        Text(
            label,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontSize = 11.sp
        )
        Text(
            value,
            color = valueColor,
            fontWeight = FontWeight.SemiBold,
            fontSize = 11.sp
        )
    }
}

private fun relationLabel(item: ShadowComparisonUi): String = when {
    !item.engineSignalFound -> "CANDIDATE ONLY"
    item.relation.contains("CANDIDATE_BEFORE", true) ||
        item.relation.contains("ENGINE_AFTER_CANDIDATE", true) -> "CANDIDATE FIRST"
    item.relation.contains("ENGINE_BEFORE", true) -> "ENGINE FIRST"
    item.relation.contains("SAME", true) -> "SAME TIME"
    else -> item.comparisonStatus
}

@Composable
private fun leadColor(value: Double?): Color = when {
    value == null -> MaterialTheme.colorScheme.onSurface
    value > 0 -> MaterialTheme.colorScheme.secondary
    value < 0 -> MaterialTheme.colorScheme.tertiary
    else -> MaterialTheme.colorScheme.onSurface
}

private fun fmtPct(value: Double?): String =
    value?.let { String.format(Locale.US, "%+.2f%%", it) } ?: "—"

private fun fmtPctPlain(value: Double?): String =
    value?.let { String.format(Locale.US, "%.2f%%", it) } ?: "—"

private fun fmtNum(value: Double?): String =
    value?.let { String.format(Locale.US, "%.3f", it) } ?: "—"

private fun fmtX(value: Double?): String =
    value?.let { String.format(Locale.US, "%.2fx", it) } ?: "—"

private fun fmtMin(value: Double): String =
    String.format(Locale.US, "%+.1f min", value)
