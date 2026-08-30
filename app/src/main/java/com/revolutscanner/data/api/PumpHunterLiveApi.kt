package com.revolutscanner.data.api

import com.revolutscanner.domain.model.LiveSignalUi
import com.revolutscanner.domain.model.LiveSnapshot
import com.revolutscanner.domain.model.ServerStatusUi
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

class PumpHunterLiveApi(
    private val baseUrlProvider: () -> String
) {

    fun fetchSnapshot(): LiveSnapshot {
        val baseUrl = normalizeBaseUrl(baseUrlProvider())
        require(baseUrl.isNotBlank()) { "Ustaw adres serwera Pump Hunter w Ustawieniach." }

        val root = fetchObject(baseUrl, "/")
        val server = ServerStatusUi(
            name = root.optString("name", "Pump Hunter Server"),
            version = root.optString("version", "-"),
            status = root.optString("status", "running"),
            engine = root.optString("engine", "-")
        )

        val signalsJson = fetchObject(baseUrl, "/signals")
        val signalsArray = signalsJson.optJSONArray("signals") ?: JSONArray()
        val signals = buildList {
            for (i in 0 until signalsArray.length()) {
                val item = signalsArray.optJSONObject(i) ?: continue
                add(parseSignal(item, i))
            }
        }.sortedByDescending { it.timestamp }

        val trackedAssets = try {
            val engine = fetchObject(baseUrl, "/v31-engine")
            engine.optJSONArray("tracked_assets")?.length()
                ?: engine.optJSONArray("assets")?.length()
                ?: 0
        } catch (_: Exception) {
            0
        }

        return LiveSnapshot(
            server = server,
            trackedAssets = trackedAssets,
            signals = signals,
            lastUpdateMillis = System.currentTimeMillis()
        )
    }

    private fun fetchObject(baseUrl: String, path: String): JSONObject {
        val connection = URL(baseUrl + path).openConnection() as HttpURLConnection
        connection.requestMethod = "GET"
        connection.connectTimeout = 8_000
        connection.readTimeout = 12_000
        connection.setRequestProperty("Accept", "application/json")
        connection.setRequestProperty("User-Agent", "PumpHunterAndroid/3.0")

        try {
            val code = connection.responseCode
            val stream = if (code in 200..299) connection.inputStream else connection.errorStream
            val body = stream?.bufferedReader()?.use { it.readText() }.orEmpty()

            if (code !in 200..299) {
                throw IllegalStateException("HTTP $code ${path}: ${body.take(180)}")
            }

            return JSONObject(body)
        } finally {
            connection.disconnect()
        }
    }

    private fun parseSignal(obj: JSONObject, index: Int): LiveSignalUi {
        val type = obj.optString("type", obj.optString("signal_type", "UNKNOWN")).uppercase()
        val asset = obj.optString("asset", "?").uppercase()
        val timestamp = longOrNow(obj, "timestamp", "signal_ts")

        val fusion = obj.optJSONObject("v32")?.optJSONObject("fusion")
        val fusionMomentum = fusion?.optJSONObject("momentum")
        val fusionVolume = fusion?.optJSONObject("volume")
        val fusionFlow = fusion?.optJSONObject("order_flow")
        val fusionContext = fusion?.optJSONObject("context")

        return LiveSignalUi(
            id = "$timestamp-$asset-$type-$index",
            type = type,
            asset = asset,
            assetName = obj.optString("asset_name", asset),
            source = obj.optString("source", "-").uppercase(),
            sourceName = obj.optString(
                "source_name",
                prettySource(obj.optString("source", "-"))
            ),
            sourceSymbol = nullableString(obj, "source_symbol"),
            price = nullableDouble(obj, "price"),
            signalPrice = nullableDouble(obj, "signal_price"),
            pumpScore = nullableInt(obj, "pump_score"),
            momentumScore = nullableInt(obj, "momentum_score"),
            dynamicMomentumScore = nullableInt(obj, "dynamic_momentum_score"),
            qualityScore = nullableInt(obj, "quality_score"),
            quality = nullableString(obj, "quality"),
            fusionScore = fusion?.let { nullableInt(it, "score") },
            fusionGrade = fusion?.let { nullableString(it, "grade") },
            dynamicPhase = nullableString(obj, "dynamic_phase"),
            dynamicVelocityPct = nullableDouble(obj, "dynamic_velocity_pct"),
            dynamicAccelerationPct = nullableDouble(obj, "dynamic_acceleration_pct"),
            moveStage = nullableString(obj, "move_stage"),
            timestamp = timestamp,
            windows = findMinuteWindows(obj),
            buyRatio1m = fusionFlow?.let { nullableDouble(it, "buy_ratio_1m") }
                ?: nullableDouble(obj, "buy_ratio_1m"),
            buyRatio5m = fusionFlow?.let { nullableDouble(it, "buy_ratio_5m") }
                ?: nullableDouble(obj, "buy_ratio_5m"),
            trades1m = fusionFlow?.let { nullableInt(it, "trades_1m") }
                ?: nullableInt(obj, "trades_1m"),
            volumeRatio = fusionVolume?.let { nullableDouble(it, "ratio") }
                ?: nullableDouble(obj, "volume_ratio"),
            volumeConfirmed = fusionVolume?.let { nullableBoolean(it, "confirmed") }
                ?: nullableBoolean(obj, "volume_confirmed"),
            change24hPct = fusionContext?.let { nullableDouble(it, "change_24h_pct") }
                ?: nullableDouble(obj, "change_24h_pct"),
            drawdown72hPct = fusionContext?.let { nullableDouble(it, "drawdown_72h_pct") }
                ?: nullableDouble(obj, "drawdown_from_72h_peak_pct")
        )
    }

    private fun findMinuteWindows(root: JSONObject): Map<Int, Double?> {
        val candidates = listOf(
            "dynamic_windows",
            "windows",
            "moves",
            "dynamic_moves",
            "changes",
            "change_windows"
        )

        for (name in candidates) {
            val obj = root.optJSONObject(name)
            if (obj != null && looksLikeMinuteWindows(obj)) {
                return minuteMap(obj)
            }
        }

        return findMinuteWindowsRecursive(root, 0) ?: emptyMap()
    }

    private fun findMinuteWindowsRecursive(obj: JSONObject, depth: Int): Map<Int, Double?>? {
        if (depth > 3) return null
        if (looksLikeMinuteWindows(obj)) return minuteMap(obj)

        val keys = obj.keys()
        while (keys.hasNext()) {
            val key = keys.next()
            val child = obj.optJSONObject(key) ?: continue
            val found = findMinuteWindowsRecursive(child, depth + 1)
            if (found != null) return found
        }
        return null
    }

    private fun looksLikeMinuteWindows(obj: JSONObject): Boolean {
        return obj.has("1m") && (obj.has("5m") || obj.has("10m") || obj.has("30m"))
    }

    private fun minuteMap(obj: JSONObject): Map<Int, Double?> =
        (1..30).associateWith { minute -> nullableDouble(obj, "${minute}m") }

    private fun nullableDouble(obj: JSONObject, key: String): Double? {
        if (!obj.has(key) || obj.isNull(key)) return null
        val value = obj.optDouble(key, Double.NaN)
        return value.takeIf { !it.isNaN() && it.isFinite() }
    }

    private fun nullableInt(obj: JSONObject, key: String): Int? {
        if (!obj.has(key) || obj.isNull(key)) return null
        return when (val raw = obj.opt(key)) {
            is Number -> raw.toInt()
            is String -> raw.toDoubleOrNull()?.toInt()
            else -> null
        }
    }

    private fun nullableBoolean(obj: JSONObject, key: String): Boolean? {
        if (!obj.has(key) || obj.isNull(key)) return null
        return when (val raw = obj.opt(key)) {
            is Boolean -> raw
            is String -> raw.equals("true", ignoreCase = true)
            else -> null
        }
    }

    private fun nullableString(obj: JSONObject, key: String): String? {
        if (!obj.has(key) || obj.isNull(key)) return null
        return obj.optString(key).takeIf { it.isNotBlank() && it != "null" }
    }

    private fun longOrNow(obj: JSONObject, vararg keys: String): Long {
        for (key in keys) {
            if (!obj.has(key) || obj.isNull(key)) continue
            when (val raw = obj.opt(key)) {
                is Number -> return raw.toLong()
                is String -> raw.toDoubleOrNull()?.toLong()?.let { return it }
            }
        }
        return System.currentTimeMillis() / 1000L
    }

    private fun prettySource(source: String): String = when (source.uppercase()) {
        "BINANCE" -> "Binance"
        "BYBIT" -> "Bybit"
        "GATE" -> "Gate.io"
        "OKX" -> "OKX"
        "KUCOIN" -> "KuCoin"
        "COINBASE" -> "Coinbase"
        "KRAKEN" -> "Kraken"
        else -> source
    }

    private fun normalizeBaseUrl(value: String): String {
        val clean = value.trim().trimEnd('/')
        if (clean.isBlank()) return ""
        return if (clean.startsWith("http://") || clean.startsWith("https://")) clean
        else "http://$clean"
    }
}
