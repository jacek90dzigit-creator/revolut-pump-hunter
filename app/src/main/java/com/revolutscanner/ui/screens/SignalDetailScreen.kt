package com.revolutscanner.ui.screens

import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.ui.components.*

@Composable
fun SignalDetailScreen(
    signal: LiveSignalUi,
    favorite: Boolean,
    onFavorite: () -> Unit,
    onBack: () -> Unit
) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                OutlinedButton(onClick = onBack) { Text("← Wróć") }
                TextButton(onClick = onFavorite) {
                    Text(
                        if (favorite) "★ Obserwowane" else "☆ Obserwuj",
                        fontSize = 16.sp
                    )
                }
            }
        }

        item {
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Column {
                    Text(signal.asset, fontSize = 34.sp, fontWeight = FontWeight.Bold)
                    Text(
                        signal.assetName,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    signal.sourceSymbol?.let {
                        Text(it, color = MaterialTheme.colorScheme.onSurfaceVariant, fontSize = 12.sp)
                    }
                }

                Column {
                    ExchangeBadge(signal.sourceName)
                    Spacer(Modifier.height(6.dp))
                    SignalTypeBadge(signal.type)
                }
            }
        }

        item {
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.surfaceVariant
                )
            ) {
                Column(Modifier.padding(16.dp)) {
                    signal.price?.let { MetricRow("Cena teraz", formatPrice(it)) }
                    signal.signalPrice?.let { MetricRow("Cena sygnału", formatPrice(it)) }
                    MetricRow("Czas sygnału", formatTime(signal.timestamp))
                    signal.dynamicPhase?.let { MetricRow("Dynamic phase", it) }
                    signal.moveStage?.let { MetricRow("Move stage", it) }
                    signal.quality?.let { MetricRow("Quality", it) }
                }
            }
        }

        item {
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.surfaceVariant
                )
            ) {
                Column(Modifier.padding(16.dp)) {
                    Text("TREND SZERSZY", fontWeight = FontWeight.Bold)
                    Spacer(Modifier.height(4.dp))
                    Text(
                        "Informacyjny kontekst ceny 1D / 3D / 5D",
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontSize = 12.sp
                    )
                    Spacer(Modifier.height(12.dp))

                    PriceContextStrip(signal.priceContext)

                    signal.priceContext?.let { context ->
                        Spacer(Modifier.height(12.dp))
                        HorizontalDivider()
                        Spacer(Modifier.height(8.dp))
                        MetricRow("Źródło kontekstu", context.sourceName)
                        context.sourceSymbol?.let { MetricRow("Para", it) }
                        MetricRow("Okno referencyjne", "±${context.windowMinutes} min")
                        MetricRow("Cache", "${context.cacheSeconds / 60} min")
                        MetricRow("Wpływa na Engine", if (context.affectsEngine) "TAK" else "NIE")
                    }
                }
            }
        }

        item {
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.surfaceVariant
                )
            ) {
                Column(Modifier.padding(16.dp)) {
                    Text("SCORING", fontWeight = FontWeight.Bold)
                    Spacer(Modifier.height(10.dp))

                    signal.fusionScore?.let {
                        ScoreBar("Fusion Score", it.coerceIn(0, 100))
                        Spacer(Modifier.height(10.dp))
                    }
                    signal.qualityScore?.let {
                        ScoreBar("Quality Score", it.coerceIn(0, 100))
                        Spacer(Modifier.height(10.dp))
                    }
                    signal.dynamicMomentumScore?.let {
                        ScoreBar("Dynamic Momentum", it.coerceIn(0, 100))
                        Spacer(Modifier.height(10.dp))
                    }
                    signal.pumpScore?.let {
                        ScoreBar("Pump Score", it.coerceIn(0, 100))
                    }

                    signal.fusionGrade?.let {
                        Spacer(Modifier.height(10.dp))
                        MetricRow("Fusion grade", it)
                    }
                }
            }
        }

        item {
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.surfaceVariant
                )
            ) {
                Column(Modifier.padding(16.dp)) {
                    Text("RUCH 1–30 MIN", fontWeight = FontWeight.Bold)
                    Spacer(Modifier.height(4.dp))
                    Text(
                        "Pełne dynamiczne okna Engine 3.2.0",
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontSize = 12.sp
                    )
                    Spacer(Modifier.height(12.dp))

                    Column(
                        Modifier.horizontalScroll(rememberScrollState())
                    ) {
                        (1..30).chunked(5).forEach { row ->
                            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                                row.forEach { minute ->
                                    Surface(
                                        color = MaterialTheme.colorScheme.surface,
                                        shape = MaterialTheme.shapes.small,
                                        modifier = Modifier.width(68.dp)
                                    ) {
                                        Column(Modifier.padding(8.dp)) {
                                            Text(
                                                "${minute}m",
                                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                                fontSize = 10.sp
                                            )
                                            Text(
                                                signal.windows[minute]?.let { formatPct(it) } ?: "—",
                                                fontSize = 12.sp,
                                                fontWeight = FontWeight.SemiBold
                                            )
                                        }
                                    }
                                }
                            }
                            Spacer(Modifier.height(8.dp))
                        }
                    }
                }
            }
        }

        item {
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.surfaceVariant
                )
            ) {
                Column(Modifier.padding(16.dp)) {
                    Text("ORDER FLOW / VOLUME / CONTEXT", fontWeight = FontWeight.Bold)
                    Spacer(Modifier.height(8.dp))
                    signal.buyRatio1m?.let { MetricRow("Buy ratio 1m", String.format("%.3f", it)) }
                    signal.buyRatio5m?.let { MetricRow("Buy ratio 5m", String.format("%.3f", it)) }
                    signal.trades1m?.let { MetricRow("Trades 1m", it.toString()) }
                    signal.volumeRatio?.let { MetricRow("Volume ratio", String.format("%.3f", it)) }
                    signal.volumeConfirmed?.let { MetricRow("Volume confirmed", if (it) "TAK" else "NIE") }
                    signal.change24hPct?.let { MetricRow("Zmiana 24h Engine", formatPct(it)) }
                    signal.drawdown72hPct?.let { MetricRow("Drawdown 72h", formatPct(it)) }
                    signal.dynamicVelocityPct?.let { MetricRow("Velocity", formatPct(it)) }
                    signal.dynamicAccelerationPct?.let { MetricRow("Acceleration", formatPct(it)) }
                }
            }
        }

        item { Spacer(Modifier.height(24.dp)) }
    }
}
