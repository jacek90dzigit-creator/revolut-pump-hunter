package com.revolutscanner.data.api

import com.revolutscanner.domain.model.ActivePumpUi
import com.revolutscanner.domain.model.DashboardStats
import com.revolutscanner.domain.model.HistoryItemUi
import com.revolutscanner.domain.model.PumpSignalUi

/**
 * Kontrakt klienta Android -> Oracle Pump Hunter.
 *
 * W pakiecie UI Foundation nie wykonujemy jeszcze prawdziwych requestów.
 * W kolejnym etapie implementacja tego interfejsu zostanie podpięta
 * do endpointów backendu 3.1.2.
 */
interface PumpHunterApi {
    suspend fun getDashboard(): DashboardStats
    suspend fun getSignals(): List<PumpSignalUi>
    suspend fun getActivePumps(): List<ActivePumpUi>
    suspend fun getHistory(): List<HistoryItemUi>
}
