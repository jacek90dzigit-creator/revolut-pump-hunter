package com.revolutscanner.data.local

import android.content.Context

class AppPreferences(context: Context) {

    private val prefs = context.getSharedPreferences("pump_hunter_android_3", Context.MODE_PRIVATE)

    var serverUrl: String
        get() = prefs.getString(KEY_SERVER_URL, "")?.trim().orEmpty()
        set(value) {
            prefs.edit().putString(KEY_SERVER_URL, normalizeUrl(value)).apply()
        }

    fun favorites(): Set<String> =
        prefs.getStringSet(KEY_FAVORITES, emptySet())
            ?.map { it.uppercase() }
            ?.toSet()
            ?: emptySet()

    fun toggleFavorite(asset: String): Set<String> {
        val symbol = asset.uppercase()
        val updated = favorites().toMutableSet()

        if (!updated.add(symbol)) {
            updated.remove(symbol)
        }

        prefs.edit().putStringSet(KEY_FAVORITES, updated).apply()
        return updated.toSet()
    }

    private fun normalizeUrl(value: String): String {
        val clean = value.trim().trimEnd('/')
        if (clean.isBlank()) return ""
        return if (clean.startsWith("http://") || clean.startsWith("https://")) {
            clean
        } else {
            "http://$clean"
        }
    }

    companion object {
        private const val KEY_SERVER_URL = "server_url"
        private const val KEY_FAVORITES = "favorites"
    }
}
