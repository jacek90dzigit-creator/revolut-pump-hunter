package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.revolutscanner.ui.components.SectionHeader
import com.revolutscanner.ui.components.ServerStatusBadge

@Composable
fun SettingsScreen(
    serverUrl: String,
    serverOnline: Boolean,
    engineVersion: String,
    errorMessage: String?,
    onSaveServerUrl: (String) -> Unit,
    onRefresh: () -> Unit
) {
    var url by remember(serverUrl) { mutableStateOf(serverUrl) }

    LazyColumn(
        modifier = Modifier.padding(horizontal = 16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            SectionHeader(
                "Ustawienia",
                "Android 3.0 LIVE • połączenie z Pump Hunter Engine"
            )
        }

        item {
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
                        Text("Serwer Oracle", style = MaterialTheme.typography.titleMedium)
                        ServerStatusBadge(serverOnline)
                    }

                    Spacer(Modifier.height(12.dp))

                    OutlinedTextField(
                        value = url,
                        onValueChange = { url = it },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        label = { Text("Adres serwera") },
                        placeholder = { Text("http://IP_SERWERA:8000") }
                    )

                    Spacer(Modifier.height(10.dp))

                    Button(
                        onClick = { onSaveServerUrl(url) },
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text("Zapisz i połącz")
                    }

                    Spacer(Modifier.height(6.dp))

                    OutlinedButton(
                        onClick = onRefresh,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text("Odśwież teraz")
                    }

                    errorMessage?.let {
                        Spacer(Modifier.height(10.dp))
                        Text(
                            it,
                            color = MaterialTheme.colorScheme.error,
                            style = MaterialTheme.typography.bodySmall
                        )
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
                    Text("Informacje", style = MaterialTheme.typography.titleMedium)
                    Spacer(Modifier.height(8.dp))
                    Text("Aplikacja: Android 3.0")
                    Text("Engine: $engineVersion")
                    Text("Odświeżanie: co 15 s")
                    Text("Źródło sygnałów: /signals")
                    Text("Stan silnika: / + /v31-engine")
                    Spacer(Modifier.height(8.dp))
                    Text(
                        "Watchlista ★ jest zapisywana lokalnie na telefonie.",
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        }

        item { Spacer(Modifier.height(24.dp)) }
    }
}
