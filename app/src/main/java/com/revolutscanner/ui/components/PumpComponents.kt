package com.revolutscanner.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.revolutscanner.domain.model.PumpStatus

@Composable
fun SectionHeader(title:String, subtitle:String?=null){
    Column(Modifier.fillMaxWidth().padding(bottom=8.dp)){
        Text(title,fontSize=22.sp,fontWeight=FontWeight.Bold)
        subtitle?.let { Spacer(Modifier.height(3.dp)); Text(it,color=MaterialTheme.colorScheme.onSurfaceVariant,fontSize=13.sp) }
    }
}
@Composable
fun DemoDataBadge(){
    Box(Modifier.background(MaterialTheme.colorScheme.tertiary.copy(alpha=.14f),RoundedCornerShape(50)).padding(horizontal=10.dp,vertical=5.dp)){
        Text("DEMO DATA",color=MaterialTheme.colorScheme.tertiary,fontWeight=FontWeight.Bold,fontSize=11.sp)
    }
}
@Composable
fun ServerStatusBadge(online:Boolean){
    val c=if(online) MaterialTheme.colorScheme.secondary else MaterialTheme.colorScheme.error
    Box(Modifier.background(c.copy(alpha=.12f),RoundedCornerShape(50)).padding(horizontal=9.dp,vertical=5.dp)){
        Text(if(online)"● ONLINE" else "● OFFLINE",color=c,fontSize=11.sp,fontWeight=FontWeight.Bold)
    }
}
@Composable
fun StatCard(value:String,label:String,modifier:Modifier=Modifier){
    Card(modifier=modifier,colors=CardDefaults.cardColors(containerColor=MaterialTheme.colorScheme.surfaceVariant),shape=RoundedCornerShape(16.dp)){
        Column(Modifier.padding(horizontal=14.dp,vertical=12.dp)){
            Text(value,fontSize=24.sp,fontWeight=FontWeight.Bold)
            Text(label,color=MaterialTheme.colorScheme.onSurfaceVariant,fontSize=12.sp)
        }
    }
}
@Composable
fun ScoreBar(label:String,score:Int,max:Int=100){
    val n=(score.toFloat()/max).coerceIn(0f,1f)
    val c=when{n>=.75f->MaterialTheme.colorScheme.secondary;n>=.5f->MaterialTheme.colorScheme.tertiary;else->MaterialTheme.colorScheme.primary}
    Column(Modifier.fillMaxWidth()){
        Row(Modifier.fillMaxWidth(),horizontalArrangement=Arrangement.SpaceBetween){
            Text(label,color=MaterialTheme.colorScheme.onSurfaceVariant,fontSize=13.sp)
            Text("$score/$max",fontWeight=FontWeight.SemiBold,fontSize=13.sp)
        }
        Spacer(Modifier.height(6.dp))
        LinearProgressIndicator(progress={n},modifier=Modifier.fillMaxWidth().height(7.dp),color=c,trackColor=MaterialTheme.colorScheme.surface)
    }
}
@Composable
fun ExchangeBadge(exchange:String){
    Box(Modifier.background(MaterialTheme.colorScheme.surface,RoundedCornerShape(8.dp)).padding(horizontal=9.dp,vertical=5.dp)){
        Text(exchange.uppercase(),fontSize=11.sp,fontWeight=FontWeight.Bold,color=MaterialTheme.colorScheme.secondary)
    }
}
@Composable
fun StatusBadge(status:PumpStatus){
    val label=when(status){PumpStatus.DETECTED->"PUMP";PumpStatus.HOLD->"HOLD";PumpStatus.WATCH->"WATCH";PumpStatus.EXIT_SOON->"EXIT SOON";PumpStatus.EXIT->"EXIT"}
    val c=when(status){PumpStatus.DETECTED->MaterialTheme.colorScheme.primary;PumpStatus.HOLD->MaterialTheme.colorScheme.secondary;PumpStatus.WATCH->MaterialTheme.colorScheme.tertiary;PumpStatus.EXIT_SOON->Color(0xFFFF8A65);PumpStatus.EXIT->MaterialTheme.colorScheme.error}
    Box(Modifier.background(c.copy(alpha=.16f),RoundedCornerShape(50)).padding(horizontal=11.dp,vertical=6.dp)){Text(label,color=c,fontWeight=FontWeight.Bold,fontSize=11.sp)}
}
@Composable
fun MetricRow(label:String,value:String,valueColor:Color=MaterialTheme.colorScheme.onSurface){
    Row(Modifier.fillMaxWidth().padding(vertical=4.dp),horizontalArrangement=Arrangement.SpaceBetween,verticalAlignment=Alignment.CenterVertically){
        Text(label,color=MaterialTheme.colorScheme.onSurfaceVariant,fontSize=13.sp);Spacer(Modifier.width(12.dp));Text(value,color=valueColor,fontWeight=FontWeight.SemiBold,fontSize=14.sp)
    }
}
