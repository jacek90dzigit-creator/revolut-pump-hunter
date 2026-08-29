package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.revolutscanner.domain.model.ActivePumpUi
import com.revolutscanner.domain.model.DashboardStats
import com.revolutscanner.ui.components.ExchangeBadge
import com.revolutscanner.ui.components.MetricRow
import com.revolutscanner.ui.components.ScoreBar
import com.revolutscanner.ui.components.SectionHeader
import com.revolutscanner.ui.components.StatCard
import com.revolutscanner.ui.components.StatusBadge

@Composable
fun DashboardScreen(
    stats: DashboardStats,
    strongestPump: ActivePumpUi?
) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            SectionHeader(
                title = "PUMP HUNTER",
                subtitle = "Backend ${stats.backendVersion}  •  ${if (stats.serverOnline) "● ONLINE" else "● OFFLINE"}"
            )
        }

        item {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(10.dp)
            ) {
                StatCard(
                    value = stats.marketsCount.toString(),
                    label = "monitorowane",
                    modifier = Modifier.weight(1f)
                )
                StatCard(
                    value = stats.activePumps.toString(),
                    label = "aktywne pumpy",
                    modifier = Modifier.weight(1f)
                )
            }
        }

        item {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(10.dp)
            ) {
                StatCard(
                    value = stats.exitAlerts.toString(),
                    label = "EXIT alert",
                    modifier = Modifier.weight(1f)
                )
                StatCard(
                    value = stats.signals24h.toString(),
                    label = "sygnały 24h",
                    modifier = Modifier.weight(1f)
                )
            }
        }

        if (strongestPump != null) {
            item {
                Spacer(modifier = Modifier.height(2.dp))
                SectionHeader(
                    title = "🔥 Najmocniejszy aktywny sygnał",
                    subtitle = "Najwyższy Pump Score w tej chwili"
                )
            }

            item {
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
                                    text = strongestPump.symbol,
                                    fontSize = 28.sp,
                                    fontWeight = FontWeight.Bold
                                )
                                Text(
                                    text = strongestPump.pair,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant
                                )
                            }
                            ExchangeBadge(strongestPump.exchange)
                        }

                        Spacer(modifier = Modifier.height(14.dp))

                        Text(
                            text = "+${"%.2f".format(strongestPump.gainFromDetectionPercent)}%",
                            color = MaterialTheme.colorScheme.secondary,
                            fontSize = 26.sp,
                            fontWeight = FontWeight.Bold
                        )

                        Spacer(modifier = Modifier.height(14.dp))

                        ScoreBar(
                            label = "Pump Score",
                            score = strongestPump.pumpScore
                        )

                        Spacer(modifier = Modifier.height(12.dp))

                        MetricRow(
                            label = "Momentum",
                            value = "${"%+.2f".format(strongestPump.momentumPercent)}%"
                        )
                        MetricRow(
                            label = "Exit Score",
                            value = "${strongestPump.exitScore}/100"
                        )

                        Spacer(modifier = Modifier.height(10.dp))
                        StatusBadge(strongestPump.status)
                    }
                }
            }
        }

        item {
            Spacer(modifier = Modifier.height(8.dp))
            Text(
                text = "Ostatnia aktualizacja: ${stats.lastUpdate}",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 12.sp
            )
            Spacer(modifier = Modifier.height(88.dp))
        }
    }
}
