package com.revolutscanner

import java.util.concurrent.ConcurrentHashMap
import kotlin.math.abs

data class ActivePump(
    val symbol: String,
    val detectedPrice: Double,
    val detectedAt: Long,
    var currentPrice: Double,
    var peakPrice: Double,
    var previousPrice: Double,
    var exitScore: Int = 0,
    var status: String = "ACTIVE"
)

data class ExitSignal(
    val symbol: String,
    val currentPrice: Double,
    val peakPrice: Double,
    val dropFromPeakPercent: Double,
    val momentumPercent: Double,
    val exitScore: Int,
    val level: String
)

object ExitEngine {

    private val activePumps =
        ConcurrentHashMap<String, ActivePump>()

    fun registerPump(signal: PumpSignal) {

        activePumps.putIfAbsent(
            signal.symbol,
            ActivePump(
                symbol = signal.symbol,
                detectedPrice = signal.currentPrice,
                detectedAt = System.currentTimeMillis(),
                currentPrice = signal.currentPrice,
                peakPrice = signal.currentPrice,
                previousPrice = signal.currentPrice
            )
        )
    }

    fun updatePrice(
        symbol: String,
        price: Double
    ): ExitSignal? {

        val pump =
            activePumps[symbol] ?: return null

        pump.previousPrice =
            pump.currentPrice

        pump.currentPrice =
            price

        if (price > pump.peakPrice) {
            pump.peakPrice = price
        }

        val dropFromPeakPercent =
            if (pump.peakPrice > 0) {
                ((pump.peakPrice - price) /
                    pump.peakPrice) * 100.0
            } else {
                0.0
            }

        val momentumPercent =
            if (pump.previousPrice > 0) {
                ((price - pump.previousPrice) /
                    pump.previousPrice) * 100.0
            } else {
                0.0
            }

        val score =
            calculateExitScore(
                dropFromPeakPercent,
                momentumPercent
            )

        pump.exitScore =
            score

        val level =
            when {
                score >= 85 ->
                    "BARDZO SILNY EXIT"
                score >= 70 ->
                    "MOCNY EXIT"
                score >= 50 ->
                    "EXIT WATCH"
                score >= 30 ->
                    "OBSERWUJ"
                else ->
                    "PUMP AKTYWNY"
            }

        pump.status =
            level

        return ExitSignal(
            symbol = symbol,
            currentPrice = price,
            peakPrice = pump.peakPrice,
            dropFromPeakPercent =
                dropFromPeakPercent,
            momentumPercent =
                momentumPercent,
            exitScore = score,
            level = level
        )
    }

    private fun calculateExitScore(
        dropFromPeak: Double,
        momentum: Double
    ): Int {

        var score = 0

        score += when {
            dropFromPeak >= 8.0 -> 55
            dropFromPeak >= 6.0 -> 45
            dropFromPeak >= 4.0 -> 35
            dropFromPeak >= 3.0 -> 25
            dropFromPeak >= 2.0 -> 15
            else -> 0
        }

        if (momentum < 0) {

            score += when {
                momentum <= -3.0 -> 35
                momentum <= -2.0 -> 25
                momentum <= -1.0 -> 15
                momentum <= -0.5 -> 10
                else -> 5
            }
        }

        return score.coerceIn(0, 100)
    }

    fun getActivePumps(): List<ActivePump> {
        return activePumps.values.toList()
    }

    fun removePump(symbol: String) {
        activePumps.remove(symbol)
    }
}
