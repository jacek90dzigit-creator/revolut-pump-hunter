package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.revolutscanner.domain.model.ActivePumpUi
import com.revolutscanner.domain.model.PumpStatus
import com.revolutscanner.ui.components.ExchangeBadge
import com.revolutscanner.ui.components.MetricRow
import com.revolutscanner.ui.components.ScoreBar
import com.revolutscanner.ui.components.SectionHeader
import com.revolutscanner.ui.components.StatusBadge

@Composable
fun ActiveScreen(pumps: List<ActivePumpUi>) {
    val sorted = pumps.sortedByDescending { it.exitScore >= 70 }

    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            SectionHeader(
                title = "📈 Aktywne",
                subtitle = "Pumpy obserwowane po wykryciu"
            )
        }

        items(
            items = sorted,
            key = { it.id }
        ) { pump ->
            val urgent = pump.status == PumpStatus.EXIT || pump.exitScore >= 70

            Card(
                colors = CardDefaults.cardColors(
                    containerColor = if (urgent) {
                        MaterialTheme.colorScheme.error.copy(alpha = 0.10f)
                    } else {
                        MaterialTheme.colorScheme.surfaceVariant
                    }
                )
            ) {
                Column(
                    modifier = Modifier.padding(16.dp)
                ) {
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.SpaceBetween
                    ) {
                        Column {
                            Text(
                                text = pump.symbol,
                                fontSize = 25.sp,
                                fontWeight = FontWeight.Bold
                            )
                            Text(
                                text = pump.pair,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                fontSize = 13.sp
                            )
                        }
                        ExchangeBadge(pump.exchange)
                    }

                    Spacer(modifier = Modifier.height(10.dp))

                    if (urgent) {
                        Text(
                            text = "🔴 EXIT ALERT",
                            color = MaterialTheme.colorScheme.error,
                            fontWeight = FontWeight.Bold,
                            fontSize = 18.sp
                        )
                        Spacer(modifier = Modifier.height(8.dp))
                    }

                    MetricRow("Cena wykrycia", formatPrice(pump.detectedPrice))
                    MetricRow("Teraz", formatPrice(pump.currentPrice))
                    MetricRow("Peak", formatPrice(pump.peakPrice))
                    MetricRow(
                        "Od wykrycia",
                        "+${"%.2f".format(pump.gainFromDetectionPercent)}%",
                        MaterialTheme.colorScheme.secondary
                    )
                    MetricRow(
                        "Od peak",
                        "-${"%.2f".format(pump.dropFromPeakPercent)}%",
                        if (urgent) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurface
                    )

                    Spacer(modifier = Modifier.height(10.dp))
                    ScoreBar("Pump Score", pump.pumpScore)
                    Spacer(modifier = Modifier.height(10.dp))
                    ScoreBar("Exit Score", pump.exitScore)
                    Spacer(modifier = Modifier.height(10.dp))

                    MetricRow(
                        "Momentum",
                        "${"%+.2f".format(pump.momentumPercent)}%",
                        if (pump.momentumPercent < 0) MaterialTheme.colorScheme.error
                        else MaterialTheme.colorScheme.secondary
                    )

                    Spacer(modifier = Modifier.height(8.dp))
                    StatusBadge(pump.status)
                }
            }
        }

        item {
            Spacer(modifier = Modifier.height(88.dp))
        }
    }
}

private fun formatPrice(price: Double): String {
    return when {
        price >= 1000 -> "$${"%.2f".format(price)}"
        price >= 1 -> "$${"%.4f".format(price)}"
        else -> "$${"%.8f".format(price)}"
    }
}
