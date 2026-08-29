package com.revolutscanner.data.repository

import com.revolutscanner.domain.model.ActivePumpUi
import com.revolutscanner.domain.model.DashboardStats
import com.revolutscanner.domain.model.HistoryItemUi
import com.revolutscanner.domain.model.PumpSignalUi
import com.revolutscanner.domain.model.PumpStatus

class MockPumpHunterRepository : PumpHunterRepository {

    override fun getDashboard() = DashboardStats(
        backendVersion = "3.1.2",
        serverOnline = true,
        marketsCount = 347,
        activePumps = 3,
        exitAlerts = 1,
        signals24h = 6,
        lastUpdate = "LIVE"
    )

    override fun getSignals() = listOf(
        PumpSignalUi(
            id = "doge-binance",
            symbol = "DOGE",
            pair = "DOGE / USDT",
            exchange = "BINANCE",
            changePercent = 8.74,
            durationMinutes = 18,
            pumpScore = 82,
            volumeScore = 91,
            momentumPercent = 7.20,
            detectedAt = "22:31",
            status = PumpStatus.HOLD
        ),
        PumpSignalUi(
            id = "pepe-bybit",
            symbol = "PEPE",
            pair = "PEPE / USDT",
            exchange = "BYBIT",
            changePercent = 6.41,
            durationMinutes = 24,
            pumpScore = 74,
            volumeScore = 79,
            momentumPercent = 5.10,
            detectedAt = "21:58",
            status = PumpStatus.WATCH
        ),
        PumpSignalUi(
            id = "sei-gate",
            symbol = "SEI",
            pair = "SEI / USDT",
            exchange = "GATE.IO",
            changePercent = 5.88,
            durationMinutes = 27,
            pumpScore = 68,
            volumeScore = 72,
            momentumPercent = 4.62,
            detectedAt = "20:47",
            status = PumpStatus.DETECTED
        )
    )

    override fun getActivePumps() = listOf(
        ActivePumpUi(
            id = "doge-binance",
            symbol = "DOGE",
            pair = "DOGE / USDT",
            exchange = "BINANCE",
            detectedPrice = 0.1921,
            currentPrice = 0.2078,
            peakPrice = 0.2134,
            gainFromDetectionPercent = 8.17,
            dropFromPeakPercent = 2.62,
            pumpScore = 82,
            exitScore = 34,
            momentumPercent = 1.17,
            status = PumpStatus.HOLD
        ),
        ActivePumpUi(
            id = "pepe-bybit",
            symbol = "PEPE",
            pair = "PEPE / USDT",
            exchange = "BYBIT",
            detectedPrice = 0.00001184,
            currentPrice = 0.00001261,
            peakPrice = 0.00001302,
            gainFromDetectionPercent = 6.50,
            dropFromPeakPercent = 3.15,
            pumpScore = 74,
            exitScore = 58,
            momentumPercent = -0.84,
            status = PumpStatus.WATCH
        ),
        ActivePumpUi(
            id = "wif-kucoin",
            symbol = "WIF",
            pair = "WIF / USDT",
            exchange = "KUCOIN",
            detectedPrice = 1.842,
            currentPrice = 1.917,
            peakPrice = 2.061,
            gainFromDetectionPercent = 4.07,
            dropFromPeakPercent = 6.99,
            pumpScore = 77,
            exitScore = 84,
            momentumPercent = -3.22,
            status = PumpStatus.EXIT
        )
    )

    override fun getHistory() = listOf(
        HistoryItemUi(
            id = "bonk",
            symbol = "BONK",
            exchange = "BINANCE",
            pumpPercent = 12.8,
            peakPercent = 18.4,
            exitPercent = 14.7,
            closedAt = "19:42"
        ),
        HistoryItemUi(
            id = "arb",
            symbol = "ARB",
            exchange = "BYBIT",
            pumpPercent = 7.2,
            peakPercent = 9.1,
            exitPercent = 5.8,
            closedAt = "16:18"
        ),
        HistoryItemUi(
            id = "sui",
            symbol = "SUI",
            exchange = "KRAKEN",
            pumpPercent = 6.4,
            peakPercent = 11.3,
            exitPercent = 8.9,
            closedAt = "13:07"
        )
    )
}
