package com.revolutscanner.data.repository

import com.revolutscanner.domain.model.ActivePumpUi
import com.revolutscanner.domain.model.DashboardStats
import com.revolutscanner.domain.model.HistoryItemUi
import com.revolutscanner.domain.model.PumpSignalUi

interface PumpHunterRepository {
    fun getDashboard(): DashboardStats
    fun getSignals(): List<PumpSignalUi>
    fun getActivePumps(): List<ActivePumpUi>
    fun getHistory(): List<HistoryItemUi>
}
