package com.revolutscanner.domain.model

data class ServerStatusUi(
    val name: String = "Pump Hunter Server",
    val version: String = "-",
    val status: String = "offline",
    val engine: String = "-"
)

data class LiveSignalUi(
    val id: String,
    val type: String,
    val asset: String,
    val assetName: String,
    val source: String,
    val sourceName: String,
    val sourceSymbol: String?,
    val price: Double?,
    val signalPrice: Double?,
    val pumpScore: Int?,
    val momentumScore: Int?,
    val dynamicMomentumScore: Int?,
    val qualityScore: Int?,
    val quality: String?,
    val fusionScore: Int?,
    val fusionGrade: String?,
    val dynamicPhase: String?,
    val dynamicVelocityPct: Double?,
    val dynamicAccelerationPct: Double?,
    val moveStage: String?,
    val timestamp: Long,
    val windows: Map<Int, Double?>,
    val buyRatio1m: Double?,
    val buyRatio5m: Double?,
    val trades1m: Int?,
    val volumeRatio: Double?,
    val volumeConfirmed: Boolean?,
    val change24hPct: Double?,
    val drawdown72hPct: Double?
) {
    val primaryScore: Int?
        get() = fusionScore ?: qualityScore ?: dynamicMomentumScore ?: pumpScore ?: momentumScore
}

data class LiveSnapshot(
    val server: ServerStatusUi,
    val trackedAssets: Int,
    val signals: List<LiveSignalUi>,
    val lastUpdateMillis: Long
)
