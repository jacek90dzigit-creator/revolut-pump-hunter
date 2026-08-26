package com.revolutscanner

import java.util.concurrent.ConcurrentHashMap

data class PricePoint(
    val timestamp: Long,
    val price: Double
)

data class PumpSignal(
    val symbol: String,
    val startPrice: Double,
    val currentPrice: Double,
    val changePercent: Double,
    val durationMinutes: Long,
    val pumpScore: Int
)

object PumpEngine {

    private const val WINDOW_MINUTES = 30L
    private const val PUMP_THRESHOLD_PERCENT = 5.0

    private val history =
        ConcurrentHashMap<String, MutableList<PricePoint>>()

    fun addPrice(
        symbol: String,
        price: Double,
        timestamp: Long = System.currentTimeMillis()
    ): PumpSignal? {

        val list = history.getOrPut(symbol) {
            mutableListOf()
        }

        synchronized(list) {

            list.add(
                PricePoint(
                    timestamp = timestamp,
                    price = price
                )
            )

            val cutoff =
                timestamp - WINDOW_MINUTES * 60_000

            list.removeAll {
                it.timestamp < cutoff
            }

            if (list.size < 2) {
                return null
            }

            val oldest = list.first()

            if (oldest.price <= 0.0) {
                return null
            }

            val changePercent =
                ((price - oldest.price) / oldest.price) * 100.0

            if (changePercent < PUMP_THRESHOLD_PERCENT) {
                return null
            }

            val durationMinutes =
                (timestamp - oldest.timestamp) / 60_000

            val pumpScore =
                calculatePumpScore(changePercent)

            return PumpSignal(
                symbol = symbol,
                startPrice = oldest.price,
                currentPrice = price,
                changePercent = changePercent,
                durationMinutes = durationMinutes,
                pumpScore = pumpScore
            )
        }
    }

    private fun calculatePumpScore(
        changePercent: Double
    ): Int {

        return when {
            changePercent >= 15.0 -> 10
            changePercent >= 12.0 -> 9
            changePercent >= 10.0 -> 8
            changePercent >= 8.0 -> 7
            changePercent >= 6.0 -> 6
            changePercent >= 5.0 -> 5
            else -> 0
        }
    }
}
