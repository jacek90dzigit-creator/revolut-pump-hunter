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
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.revolutscanner.ui.components.MetricRow
import com.revolutscanner.ui.components.SectionHeader

@Composable
fun SettingsScreen() {
    var pumpNotifications by remember { mutableStateOf(true) }
    var exitNotifications by remember { mutableStateOf(true) }
    var vibration by remember { mutableStateOf(true) }

    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            SectionHeader(
                title = "⚙️ Ustawienia",
                subtitle = "Ustawienia aplikacji, nie silnika backendu"
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
                    SettingSwitch(
                        label = "Powiadomienia PUMP",
                        checked = pumpNotifications,
                        onCheckedChange = { pumpNotifications = it }
                    )
                    SettingSwitch(
                        label = "Powiadomienia EXIT",
                        checked = exitNotifications,
                        onCheckedChange = { exitNotifications = it }
                    )
                    SettingSwitch(
                        label = "Wibracja",
                        checked = vibration,
                        onCheckedChange = { vibration = it }
                    )
                }
            }
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
                    MetricRow("Minimalny Pump Score", "60")
                    MetricRow("Minimalny Exit Score", "50")
                    MetricRow("Motyw", "Dark")
                    MetricRow("Warstwa danych", "MOCK UI")
                    MetricRow("Backend", "3.1.2")
                }
            }
        }

        item {
            Spacer(modifier = Modifier.height(88.dp))
        }
    }
}

@Composable
private fun SettingSwitch(
    label: String,
    checked: Boolean,
    onCheckedChange: (Boolean) -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.SpaceBetween
    ) {
        Text(text = label)
        Switch(
            checked = checked,
            onCheckedChange = onCheckedChange
        )
    }
}
