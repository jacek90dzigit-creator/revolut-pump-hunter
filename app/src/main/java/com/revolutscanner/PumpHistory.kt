package com.revolutscanner

import java.util.concurrent.ConcurrentHashMap

data class PumpHistoryPoint(
    val timestamp: Long,
    val price: Double
)

object PumpHistory {

    private const val MAX_POINTS_PER_PUMP = 720

    private val history =
        ConcurrentHashMap<String, MutableList<PumpHistoryPoint>>()

    fun startTracking(
        symbol: String,
        price: Double,
        timestamp: Long = System.currentTimeMillis()
    ) {
        val list = history.getOrPut(symbol) {
            mutableListOf()
        }

        synchronized(list) {
            if (list.isEmpty()) {
                list.add(
                    PumpHistoryPoint(
                        timestamp = timestamp,
                        price = price
                    )
                )
            }
        }
    }

    fun addPrice(
        symbol: String,
        price: Double,
        timestamp: Long = System.currentTimeMillis()
    ) {
        val list = history[symbol] ?: return

        synchronized(list) {

            list.add(
                PumpHistoryPoint(
                    timestamp = timestamp,
                    price = price
                )
            )

            if (list.size > MAX_POINTS_PER_PUMP) {
                val excess =
                    list.size - MAX_POINTS_PER_PUMP

                repeat(excess) {
                    list.removeAt(0)
                }
            }
        }
    }

    fun getHistory(
        symbol: String
    ): List<PumpHistoryPoint> {

        val list =
            history[symbol] ?: return emptyList()

        synchronized(list) {
            return list.toList()
        }
    }

    fun stopTracking(
        symbol: String
    ) {
        history.remove(symbol)
    }

    fun isTracking(
        symbol: String
    ): Boolean {
        return history.containsKey(symbol)
    }
}
