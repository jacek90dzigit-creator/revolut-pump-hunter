package com.revolutscanner.domain.model

data class DashboardStats(
    val backendVersion: String,
    val serverOnline: Boolean,
    val marketsCount: Int,
    val activePumps: Int,
    val exitAlerts: Int,
    val signals24h: Int,
    val lastUpdate: String
)

data class PumpSignalUi(
    val id: String,
    val symbol: String,
    val pair: String,
    val exchange: String,
    val changePercent: Double,
    val durationMinutes: Int,
    val pumpScore: Int,
    val volumeScore: Int,
    val momentumPercent: Double,
    val detectedAt: String,
    val status: PumpStatus
)

data class ActivePumpUi(
    val id: String,
    val symbol: String,
    val pair: String,
    val exchange: String,
    val detectedPrice: Double,
    val currentPrice: Double,
    val peakPrice: Double,
    val gainFromDetectionPercent: Double,
    val dropFromPeakPercent: Double,
    val pumpScore: Int,
    val exitScore: Int,
    val momentumPercent: Double,
    val status: PumpStatus
)

data class HistoryItemUi(
    val id: String,
    val symbol: String,
    val exchange: String,
    val pumpPercent: Double,
    val peakPercent: Double,
    val exitPercent: Double,
    val closedAt: String
)

enum class PumpStatus {
    DETECTED,
    HOLD,
    WATCH,
    EXIT_SOON,
    EXIT
}
