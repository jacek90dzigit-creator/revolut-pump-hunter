package com.revolutscanner.ui.theme

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color

private val PumpHunterDarkColors = darkColorScheme(
    primary = Color(0xFFFF6B3D),
    secondary = Color(0xFF3EE6A8),
    tertiary = Color(0xFFFFC857),
    background = Color(0xFF080A0D),
    surface = Color(0xFF10141A),
    surfaceVariant = Color(0xFF171C24),
    onPrimary = Color.Black,
    onSecondary = Color.Black,
    onBackground = Color(0xFFF2F4F7),
    onSurface = Color(0xFFF2F4F7),
    onSurfaceVariant = Color(0xFFA9B2C0),
    error = Color(0xFFFF5D73)
)

@Composable
fun PumpHunterTheme(
    content: @Composable () -> Unit
) {
    MaterialTheme(
        colorScheme = PumpHunterDarkColors,
        content = content
    )
}
