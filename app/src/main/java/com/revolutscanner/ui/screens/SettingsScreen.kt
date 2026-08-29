package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.revolutscanner.ui.components.*

@Composable
fun SettingsScreen(){
    var pump by remember{mutableStateOf(true)};var exit by remember{mutableStateOf(true)};var vibration by remember{mutableStateOf(true)}
    LazyColumn(modifier=Modifier.padding(horizontal=16.dp),verticalArrangement=Arrangement.spacedBy(12.dp)){
        item{SectionHeader("⚙️ Ustawienia","Ustawienia aplikacji, nie silnika backendu");DemoDataBadge()}
        item{Card(colors=CardDefaults.cardColors(containerColor=MaterialTheme.colorScheme.surfaceVariant)){Column(Modifier.padding(16.dp)){SettingSwitch("Powiadomienia PUMP",pump){pump=it};SettingSwitch("Powiadomienia EXIT",exit){exit=it};SettingSwitch("Wibracja",vibration){vibration=it}}}}
        item{Card(colors=CardDefaults.cardColors(containerColor=MaterialTheme.colorScheme.surfaceVariant)){Column(Modifier.padding(16.dp)){MetricRow("Minimalny Pump Score","60");MetricRow("Minimalny Exit Score","50");MetricRow("Motyw","Dark");MetricRow("Warstwa danych","DEMO / MOCK");MetricRow("Backend docelowy","3.1.2");MetricRow("Wersja UI","Android 2.1")}}}
        item{Text("Android 2.2: podłączenie prawdziwych danych z Oracle.",color=MaterialTheme.colorScheme.onSurfaceVariant);Spacer(Modifier.height(24.dp))}
    }
}
@Composable private fun SettingSwitch(label:String,checked:Boolean,onCheckedChange:(Boolean)->Unit){
    Row(Modifier.fillMaxWidth().padding(vertical=6.dp),verticalAlignment=Alignment.CenterVertically,horizontalArrangement=Arrangement.SpaceBetween){Text(label);Switch(checked=checked,onCheckedChange=onCheckedChange)}
}
