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
import com.revolutscanner.domain.model.HistoryItemUi
import com.revolutscanner.ui.components.ExchangeBadge
import com.revolutscanner.ui.components.MetricRow
import com.revolutscanner.ui.components.SectionHeader

@Composable
fun HistoryScreen(history: List<HistoryItemUi>) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            SectionHeader(
                title = "🕘 Historia",
                subtitle = "Zamknięte sygnały i skuteczność EXIT"
            )
        }

        items(
            items = history,
            key = { it.id }
        ) { item ->
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
                        Text(
                            text = item.symbol,
                            fontSize = 23.sp,
                            fontWeight = FontWeight.Bold
                        )
                        ExchangeBadge(item.exchange)
                    }

                    Spacer(modifier = Modifier.height(10.dp))
                    MetricRow("Pump", "+${"%.2f".format(item.pumpPercent)}%")
                    MetricRow("Peak", "+${"%.2f".format(item.peakPercent)}%")
                    MetricRow(
                        "EXIT",
                        "+${"%.2f".format(item.exitPercent)}%",
                        MaterialTheme.colorScheme.secondary
                    )
                    MetricRow("Zamknięto", item.closedAt)
                }
            }
        }

        item {
            Spacer(modifier = Modifier.height(88.dp))
        }
    }
}
