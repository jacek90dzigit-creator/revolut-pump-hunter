package com.revolutscanner.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
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
import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.domain.model.PumpStatus
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

@Composable
fun SectionHeader(title: String, subtitle: String? = null) {
    Column(Modifier.fillMaxWidth().padding(bottom = 8.dp)) {
        Text(title, fontSize = 22.sp, fontWeight = FontWeight.Bold)
        subtitle?.let {
            Spacer(Modifier.height(3.dp))
            Text(it, color = MaterialTheme.colorScheme.onSurfaceVariant, fontSize = 13.sp)
        }
    }
}

@Composable
fun ServerStatusBadge(online: Boolean) {
    val color = if (online) MaterialTheme.colorScheme.secondary else MaterialTheme.colorScheme.error
    Box(
        Modifier
            .background(color.copy(alpha = 0.14f), RoundedCornerShape(50))
            .padding(horizontal = 10.dp, vertical = 5.dp)
    ) {
        Text(
            if (online) "● ONLINE" else "● OFFLINE",
            color = color,
            fontSize = 11.sp,
            fontWeight = FontWeight.Bold
        )
    }
}

@Composable
fun StatCard(value: String, label: String, modifier: Modifier = Modifier) {
    Card(
        modifier = modifier,
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant),
        shape = RoundedCornerShape(16.dp)
    ) {
        Column(Modifier.padding(horizontal = 14.dp, vertical = 12.dp)) {
            Text(value, fontSize = 24.sp, fontWeight = FontWeight.Bold)
            Text(label, color = MaterialTheme.colorScheme.onSurfaceVariant, fontSize = 12.sp)
        }
    }
}

@Composable
fun ScoreBar(label: String, score: Int, max: Int = 100) {
    val normalized = (score.toFloat() / max.toFloat()).coerceIn(0f, 1f)
    val barColor = when {
        normalized >= .75f -> MaterialTheme.colorScheme.secondary
        normalized >= .50f -> MaterialTheme.colorScheme.tertiary
        else -> MaterialTheme.colorScheme.primary
    }

    Column(Modifier.fillMaxWidth()) {
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            Text(label, color = MaterialTheme.colorScheme.onSurfaceVariant, fontSize = 13.sp)
            Text("$score/$max", fontWeight = FontWeight.SemiBold, fontSize = 13.sp)
        }
        Spacer(Modifier.height(6.dp))
        Box(
            Modifier
                .fillMaxWidth()
                .height(7.dp)
                .background(MaterialTheme.colorScheme.surface, RoundedCornerShape(50))
        ) {
            Box(
                Modifier
                    .fillMaxWidth(normalized)
                    .fillMaxHeight()
                    .background(barColor, RoundedCornerShape(50))
            )
        }
    }
}

@Composable
fun ExchangeBadge(exchange: String) {
    Box(
        Modifier
            .background(MaterialTheme.colorScheme.surface, RoundedCornerShape(8.dp))
            .padding(horizontal = 9.dp, vertical = 5.dp)
    ) {
        Text(
            exchange.uppercase(),
            fontSize = 11.sp,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.secondary
        )
    }
}

@Composable
fun SignalTypeBadge(type: String) {
    val color = signalColor(type)
    Box(
        Modifier
            .background(color.copy(alpha = 0.16f), RoundedCornerShape(50))
            .padding(horizontal = 10.dp, vertical = 5.dp)
    ) {
        Text(type.replace("_", " "), color = color, fontWeight = FontWeight.Bold, fontSize = 11.sp)
    }
}

@Composable
fun LiveSignalCard(
    signal: LiveSignalUi,
    favorite: Boolean,
    onFavorite: () -> Unit,
    onClick: () -> Unit,
    modifier: Modifier = Modifier
) {
    Card(
        modifier = modifier
            .fillMaxWidth()
            .clickable(onClick = onClick),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)
    ) {
        Column(Modifier.padding(14.dp)) {
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.Top
            ) {
                Column {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Text(signal.asset, fontSize = 24.sp, fontWeight = FontWeight.Bold)
                        Spacer(Modifier.width(8.dp))
                        SignalTypeBadge(signal.type)
                    }
                    Text(
                        signal.assetName,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontSize = 12.sp
                    )
                }
                TextButton(onClick = onFavorite, contentPadding = PaddingValues(4.dp)) {
                    Text(
                        if (favorite) "★" else "☆",
                        fontSize = 26.sp,
                        color = if (favorite) MaterialTheme.colorScheme.tertiary
                        else MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }

            Spacer(Modifier.height(8.dp))

            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                ExchangeBadge(signal.sourceName)
                signal.price?.let {
                    Text(formatPrice(it), fontWeight = FontWeight.SemiBold)
                }
            }

            Spacer(Modifier.height(10.dp))

            val score = signal.primaryScore
            if (score != null) {
                ScoreBar(
                    label = if (signal.fusionScore != null) "Fusion Score" else "Score",
                    score = score.coerceIn(0, 100)
                )
                Spacer(Modifier.height(9.dp))
            }

            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                CompactMetric("1m", signal.windows[1]?.let { formatPct(it) } ?: "—", Modifier.weight(1f))
                CompactMetric("5m", signal.windows[5]?.let { formatPct(it) } ?: "—", Modifier.weight(1f))
                CompactMetric("10m", signal.windows[10]?.let { formatPct(it) } ?: "—", Modifier.weight(1f))
                CompactMetric("30m", signal.windows[30]?.let { formatPct(it) } ?: "—", Modifier.weight(1f))
            }

            Spacer(Modifier.height(7.dp))
            Text(
                "Źródło: ${signal.sourceName}  •  ${formatTime(signal.timestamp)}",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 11.sp
            )
        }
    }
}

@Composable
fun CompactMetric(label: String, value: String, modifier: Modifier = Modifier) {
    Column(modifier) {
        Text(label, color = MaterialTheme.colorScheme.onSurfaceVariant, fontSize = 10.sp)
        Text(value, fontSize = 13.sp, fontWeight = FontWeight.SemiBold)
    }
}

@Composable
fun MetricRow(
    label: String,
    value: String,
    valueColor: Color = MaterialTheme.colorScheme.onSurface
) {
    Row(
        Modifier.fillMaxWidth().padding(vertical = 4.dp),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text(label, color = MaterialTheme.colorScheme.onSurfaceVariant, fontSize = 13.sp)
        Spacer(Modifier.width(12.dp))
        Text(value, color = valueColor, fontWeight = FontWeight.SemiBold, fontSize = 14.sp)
    }
}

/* Zachowane dla starych, nieużywanych ekranów z gałęzi 2.x. */
@Composable
fun DemoDataBadge() {
    Box(
        Modifier
            .background(MaterialTheme.colorScheme.tertiary.copy(alpha = 0.14f), RoundedCornerShape(50))
            .padding(horizontal = 10.dp, vertical = 5.dp)
    ) {
        Text("DEMO DATA", color = MaterialTheme.colorScheme.tertiary, fontWeight = FontWeight.Bold, fontSize = 11.sp)
    }
}

@Composable
fun StatusBadge(status: PumpStatus) {
    val label = when (status) {
        PumpStatus.DETECTED -> "PUMP"
        PumpStatus.HOLD -> "HOLD"
        PumpStatus.WATCH -> "WATCH"
        PumpStatus.EXIT_SOON -> "EXIT SOON"
        PumpStatus.EXIT -> "EXIT"
    }
    Box(
        Modifier
            .background(MaterialTheme.colorScheme.primary.copy(alpha = 0.16f), RoundedCornerShape(50))
            .padding(horizontal = 10.dp, vertical = 5.dp)
    ) {
        Text(label, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.Bold, fontSize = 11.sp)
    }
}

fun formatPrice(price: Double): String = when {
    price >= 1000 -> "$${String.format(Locale.US, "%.2f", price)}"
    price >= 1 -> "$${String.format(Locale.US, "%.4f", price)}"
    price >= 0.01 -> "$${String.format(Locale.US, "%.6f", price)}"
    else -> "$${String.format(Locale.US, "%.8f", price)}"
}

fun formatPct(value: Double): String =
    String.format(Locale.US, "%+.2f%%", value)

fun formatTime(epochSeconds: Long): String {
    val millis = if (epochSeconds > 10_000_000_000L) epochSeconds else epochSeconds * 1000L
    return SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(Date(millis))
}

fun signalColor(type: String): Color = when (type.uppercase()) {
    "PUMP" -> Color(0xFF3EE6A8)
    "RE_ENTRY" -> Color(0xFF56D5FF)
    "EARLY_MOVE" -> Color(0xFFFFC857)
    "COOLING" -> Color(0xFFFF9B54)
    "EXIT" -> Color(0xFFFF5D73)
    else -> Color(0xFFA9B2C0)
}
