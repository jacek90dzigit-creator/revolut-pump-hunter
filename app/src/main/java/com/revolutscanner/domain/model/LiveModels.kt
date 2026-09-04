package com.revolutscanner.domain.model

data class ServerStatusUi(
    val name: String = "Pump Hunter Server",
    val version: String = "-",
    val status: String = "offline",
    val engine: String = "-"
)

data class PriceContextPeriodUi(
    val changePct: Double?,
    val referencePrice: Double?,
    val ready: Boolean,
    val samples: Int?,
    val error: String?
)

data class PriceContextUi(
    val asset: String,
    val assetName: String,
    val source: String,
    val sourceName: String,
    val sourceSymbol: String?,
    val currentPrice: Double?,
    val generatedAt: Long,
    val windowMinutes: Int,
    val cacheSeconds: Int,
    val informationalOnly: Boolean,
    val affectsEngine: Boolean,
    val oneDay: PriceContextPeriodUi?,
    val threeDays: PriceContextPeriodUi?,
    val fiveDays: PriceContextPeriodUi?,
    val ready: Boolean,
    val cached: Boolean,
    val cacheAgeSeconds: Double?
)

data class ShadowOutcomeUi(
    val horizonMinutes: Int,
    val changePct: Double?,
    val maxGainPct: Double?,
    val maxDrawdownPct: Double?
)

data class ShadowComparisonUi(
    val eventId: String,
    val symbol: String,
    val asset: String,
    val candidateTimestampUtc: String?,
    val candidateTs: Long?,
    val rvol15m: Double?,
    val rvol1h: Double?,
    val rvolRisingStreak: Int?,
    val rvolAccel: Double?,
    val compressionRatio: Double?,
    val rangeExpansion: Double?,
    val price15mPct: Double?,
    val price1hPct: Double?,
    val takerBuyRatio: Double?,
    val btc1hPct: Double?,
    val engineSignalFound: Boolean,
    val engineSignalType: String?,
    val engineSignalTimestampUtc: String?,
    val relation: String,
    val deltaMinutes: Double?,
    val leadMinutes: Double?,
    val engineQualityScore: Int?,
    val engineEntryStatus: String?,
    val engineMoveStage: String?,
    val fusionScore: Int?,
    val fusionGrade: String?,
    val fusionAllowed: Boolean?,
    val fusionReason: String?,
    val phaseBefore: String?,
    val phaseAfter: String?,
    val comparisonStatus: String,
    val outcomes: List<ShadowOutcomeUi>
)

data class ShadowSnapshotUi(
    val candidateName: String = "Candidate #5G",
    val p21Status: String = "-",
    val p22Status: String = "-",
    val shadowOnly: Boolean = true,
    val candidateEvents: Int = 0,
    val comparisonsComplete: Int = 0,
    val comparisonsPending: Int = 0,
    val candidateBeforeEngine: Int = 0,
    val engineBeforeCandidate: Int = 0,
    val sameTimestamp: Int = 0,
    val averageCandidateLeadMinutes: Double? = null,
    val updatedAt: String? = null,
    val comparisons: List<ShadowComparisonUi> = emptyList()
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
    val drawdown72hPct: Double?,
    val priceContext: PriceContextUi? = null
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
