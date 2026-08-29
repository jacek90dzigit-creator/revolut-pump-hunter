package com.revolutscanner.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.revolutscanner.domain.model.ActivePumpUi
import com.revolutscanner.domain.model.DashboardStats
import com.revolutscanner.ui.components.*

@Composable
fun DashboardScreen(stats:DashboardStats,strongestPump:ActivePumpUi?){
    LazyColumn(modifier=Modifier.padding(horizontal=16.dp),verticalArrangement=Arrangement.spacedBy(10.dp)){
        item{
            SectionHeader("PUMP HUNTER","Android 2.1  •  Backend ${stats.backendVersion}")
            Row(horizontalArrangement=Arrangement.spacedBy(8.dp)){DemoDataBadge();ServerStatusBadge(stats.serverOnline)}
        }
        item{
            Row(Modifier.fillMaxWidth(),horizontalArrangement=Arrangement.spacedBy(8.dp)){
                StatCard(stats.marketsCount.toString(),"monitorowane",Modifier.weight(1f))
                StatCard(stats.activePumps.toString(),"aktywne pumpy",Modifier.weight(1f))
            }
        }
        item{
            Row(Modifier.fillMaxWidth(),horizontalArrangement=Arrangement.spacedBy(8.dp)){
                StatCard(stats.exitAlerts.toString(),"EXIT alert",Modifier.weight(1f))
                StatCard(stats.signals24h.toString(),"sygnały 24h",Modifier.weight(1f))
            }
        }
        strongestPump?.let{pump->
            item{Spacer(Modifier.height(2.dp));SectionHeader("🔥 Najmocniejszy aktywny sygnał","Najwyższy Pump Score • dane demonstracyjne")}
            item{
                Card(colors=CardDefaults.cardColors(containerColor=MaterialTheme.colorScheme.surfaceVariant)){
                    Column(Modifier.padding(16.dp)){
                        Row(Modifier.fillMaxWidth(),horizontalArrangement=Arrangement.SpaceBetween){
                            Column{Text(pump.symbol,fontSize=28.sp,fontWeight=FontWeight.Bold);Text(pump.pair,color=MaterialTheme.colorScheme.onSurfaceVariant)}
                            ExchangeBadge(pump.exchange)
                        }
                        Spacer(Modifier.height(10.dp))
                        Text("${"%+.2f".format(pump.gainFromDetectionPercent)}%",color=if(pump.gainFromDetectionPercent>=0)MaterialTheme.colorScheme.secondary else MaterialTheme.colorScheme.error,fontSize=27.sp,fontWeight=FontWeight.Bold)
                        Spacer(Modifier.height(12.dp));ScoreBar("Pump Score",pump.pumpScore)
                        Spacer(Modifier.height(10.dp));MetricRow("Momentum","${"%+.2f".format(pump.momentumPercent)}%");MetricRow("Exit Score","${pump.exitScore}/100")
                        Spacer(Modifier.height(8.dp));StatusBadge(pump.status)
                    }
                }
            }
        }
        item{
            Text("Ostatnia aktualizacja: ${stats.lastUpdate} • DEMO",color=MaterialTheme.colorScheme.onSurfaceVariant,fontSize=12.sp)
            Spacer(Modifier.height(24.dp))
        }
    }
}
