package com.revolutscanner.ui.screens

import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.ui.components.LiveSignalCard
import com.revolutscanner.ui.components.SectionHeader

@Composable
fun SignalsScreen(
    signals: List<LiveSignalUi>,
    isFavorite: (String) -> Boolean,
    onFavorite: (String) -> Unit,
    onSignalClick: (LiveSignalUi) -> Unit
) {
    var filter by remember { mutableStateOf("ALL") }
    val filters = listOf("ALL", "EARLY_MOVE", "PUMP", "RE_ENTRY", "COOLING", "EXIT")
    val visible = if (filter == "ALL") signals else signals.filter { it.type == filter }

    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            SectionHeader("Sygnały LIVE", "Ostatnie zdarzenia zwrócone przez Engine 3.2.0")
        }

        item {
            Row(
                Modifier.horizontalScroll(rememberScrollState()),
                horizontalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                filters.forEach { item ->
                    FilterChip(
                        selected = filter == item,
                        onClick = { filter = item },
                        label = { Text(item.replace("_", " ")) }
                    )
                }
            }
        }

        if (visible.isEmpty()) {
            item {
                Text(
                    "Brak sygnałów dla wybranego filtra.",
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }

        items(visible, key = { it.id }) { signal ->
            LiveSignalCard(
                signal = signal,
                favorite = isFavorite(signal.asset),
                onFavorite = { onFavorite(signal.asset) },
                onClick = { onSignalClick(signal) }
            )
        }

        item { Spacer(Modifier.height(24.dp)) }
    }
}
