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
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.revolutscanner.domain.model.PumpSignalUi
import com.revolutscanner.ui.components.ExchangeBadge
import com.revolutscanner.ui.components.MetricRow
import com.revolutscanner.ui.components.ScoreBar
import com.revolutscanner.ui.components.SectionHeader
import com.revolutscanner.ui.components.StatusBadge

@Composable
fun SignalsScreen(signals: List<PumpSignalUi>) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            SectionHeader(
                title = "🚀 Sygnały",
                subtitle = "Najnowsze wykrycia Pump Huntera"
            )
        }

        items(
            items = signals,
            key = { it.id }
        ) { signal ->
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.surfaceVariant
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
                                text = signal.symbol,
                                fontSize = 24.sp,
                                fontWeight = FontWeight.Bold
                            )
                            Text(
                                text = signal.pair,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                fontSize = 13.sp
                            )
                        }
                        ExchangeBadge(signal.exchange)
                    }

                    Spacer(modifier = Modifier.height(10.dp))

                    Text(
                        text = "+${"%.2f".format(signal.changePercent)}% / ${signal.durationMinutes} min",
                        color = MaterialTheme.colorScheme.secondary,
                        fontWeight = FontWeight.Bold,
                        fontSize = 20.sp
                    )

                    Spacer(modifier = Modifier.height(12.dp))
                    ScoreBar("Pump Score", signal.pumpScore)
                    Spacer(modifier = Modifier.height(10.dp))
                    ScoreBar("Volume Score", signal.volumeScore)
                    Spacer(modifier = Modifier.height(10.dp))

                    MetricRow(
                        label = "Momentum",
                        value = "${"%+.2f".format(signal.momentumPercent)}%"
                    )
                    MetricRow(
                        label = "Wykryto",
                        value = signal.detectedAt
                    )

                    Spacer(modifier = Modifier.height(8.dp))
                    StatusBadge(signal.status)
                }
            }
        }

        item {
            Spacer(modifier = Modifier.height(88.dp))
        }
    }
}
