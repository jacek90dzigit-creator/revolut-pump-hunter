package com.revolutscanner.ui.main

import androidx.lifecycle.ViewModel
import com.revolutscanner.data.repository.MockPumpHunterRepository
import com.revolutscanner.data.repository.PumpHunterRepository

class MainViewModel(
    private val repository: PumpHunterRepository = MockPumpHunterRepository()
) : ViewModel() {

    val dashboard = repository.getDashboard()
    val signals = repository.getSignals()
    val activePumps = repository.getActivePumps()
    val history = repository.getHistory()
}
