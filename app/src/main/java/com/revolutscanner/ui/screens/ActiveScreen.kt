package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.ui.components.LiveSignalCard
import com.revolutscanner.ui.components.SectionHeader

@Composable
fun ActiveScreen(
    signals: List<LiveSignalUi>,
    isFavorite: (String) -> Boolean,
    onFavorite: (String) -> Unit,
    onSignalClick: (LiveSignalUi) -> Unit
) {
    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            SectionHeader(
                "Aktywne",
                "Najnowszy stan każdego aktywa: EARLY MOVE / PUMP / COOLING / RE-ENTRY"
            )
        }

        if (signals.isEmpty()) {
            item {
                Text("Brak aktywnych ruchów.", color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }

        items(signals, key = { it.id }) { signal ->
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
