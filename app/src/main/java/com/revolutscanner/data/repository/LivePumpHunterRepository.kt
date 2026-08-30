package com.revolutscanner.data.repository

import com.revolutscanner.data.api.PumpHunterLiveApi
import com.revolutscanner.domain.model.LiveSnapshot

class LivePumpHunterRepository(
    private val api: PumpHunterLiveApi
) {
    fun refresh(): LiveSnapshot = api.fetchSnapshot()
}
