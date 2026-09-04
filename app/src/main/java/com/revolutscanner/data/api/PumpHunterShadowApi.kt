package com.revolutscanner.data.api

import com.revolutscanner.domain.model.ShadowComparisonUi
import com.revolutscanner.domain.model.ShadowOutcomeUi
import com.revolutscanner.domain.model.ShadowSnapshotUi
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

class PumpHunterShadowApi(
    private val baseUrlProvider: () -> String
) {

    fun fetchShadow(): ShadowSnapshotUi {
        val baseUrl = normalizeBaseUrl(baseUrlProvider())
        require(baseUrl.isNotBlank()) { "Brak adresu serwera." }

        val root = fetchObject(baseUrl, "/app-shadow")
        val status = root.optJSONObject("status") ?: root
        val summary = root.optJSONObject("summary") ?: root
        val data = root.optJSONObject("data") ?: root

        val comparisonsArray =
            root.optJSONArray("comparisons")
                ?: data.optJSONArray("comparisons")
                ?: JSONArray()

        val comparisons = buildList {
            for (i in 0 until comparisonsArray.length()) {
                comparisonsArray.optJSONObject(i)?.let { add(parseComparison(it, i)) }
            }
        }.sortedByDescending { it.candidateTs ?: 0L }

        return ShadowSnapshotUi(
            candidateName = firstString(
                root, "candidate_name", "candidate", "name"
            ) ?: "Candidate #5G",
            p21Status = firstString(
                status, "p21_status", "p21", "forward_oos_status"
            ) ?: "-",
            p22Status = firstString(
                status, "p22_status", "p22_1_status", "shadow_status", "status"
            ) ?: "-",
            shadowOnly = firstBoolean(
                root, "shadow_only"
            ) ?: firstBoolean(status, "shadow_only") ?: true,
            candidateEvents = firstInt(
                summary, "candidate_events", "candidate_fresh_oos_events", "events"
            ) ?: comparisons.size,
            comparisonsComplete = firstInt(
                summary, "comparisons_complete", "complete"
            ) ?: comparisons.count { it.comparisonStatus.uppercase() == "COMPLETE" },
            comparisonsPending = firstInt(
                summary, "comparisons_pending", "pending"
            ) ?: comparisons.count { it.comparisonStatus.uppercase() != "COMPLETE" },
            candidateBeforeEngine = firstInt(
                summary,
                "candidate_before_engine",
                "candidate_earlier_than_engine",
                "candidate_earlier"
            ) ?: comparisons.count { relationKind(it) == "CANDIDATE_BEFORE_ENGINE" },
            engineBeforeCandidate = firstInt(
                summary,
                "engine_before_candidate",
                "engine_earlier_than_candidate",
                "engine_earlier"
            ) ?: comparisons.count { relationKind(it) == "ENGINE_BEFORE_CANDIDATE" },
            sameTimestamp = firstInt(
                summary, "same_timestamp", "same_time"
            ) ?: comparisons.count { relationKind(it) == "SAME_TIMESTAMP" },
            averageCandidateLeadMinutes = firstDouble(
                summary,
                "average_candidate_lead_minutes",
                "average_signed_candidate_lead_minutes",
                "average_lead_minutes"
            ),
            updatedAt = firstString(
                root, "updated_at", "generated_at", "cycle_finished_utc"
            ) ?: firstString(status, "updated_at", "cycle_finished_utc"),
            comparisons = comparisons
        )
    }

    private fun parseComparison(obj: JSONObject, index: Int): ShadowComparisonUi {
        val eventId = firstString(
            obj, "candidate_event_id", "event_id", "id"
        ) ?: "shadow-$index"

        val symbol = firstString(
            obj, "symbol", "candidate_symbol"
        ) ?: "?"

        val asset = firstString(
            obj, "asset"
        ) ?: symbol
            .removeSuffix("USDT")
            .removeSuffix("USDC")
            .removeSuffix("FDUSD")

        val relationRaw = firstString(
            obj,
            "relation",
            "temporal_relation",
            "comparison_relation",
            "assignment_relation"
        )

        val delta = firstDouble(
            obj,
            "engine_first_signal_delta_minutes",
            "delta_minutes",
            "engine_signal_delta_minutes"
        )

        val lead = firstDouble(
            obj,
            "candidate_lead_vs_engine_minutes",
            "lead_minutes",
            "candidate_lead_minutes"
        ) ?: delta

        val engineFound = firstBoolean(
            obj, "engine_signal_found"
        ) ?: (
            firstString(obj, "engine_first_signal_type", "engine_signal_type") != null
        )

        val relation = relationRaw ?: when {
            !engineFound -> "CANDIDATE_ONLY_NO_ENGINE_AFTER"
            lead == null -> "COMPARED"
            lead > 0.001 -> "CANDIDATE_BEFORE_ENGINE"
            lead < -0.001 -> "ENGINE_BEFORE_CANDIDATE"
            else -> "SAME_TIMESTAMP"
        }

        return ShadowComparisonUi(
            eventId = eventId,
            symbol = symbol,
            asset = asset,
            candidateTimestampUtc = firstString(
                obj, "candidate_signal_timestamp_utc", "candidate_timestamp_utc"
            ),
            candidateTs = firstLong(
                obj, "candidate_signal_ts", "candidate_ts"
            ),
            rvol15m = firstDouble(obj, "candidate_rvol_15m", "rvol_15m"),
            rvol1h = firstDouble(obj, "candidate_rvol_1h", "rvol_1h"),
            rvolRisingStreak = firstInt(
                obj, "candidate_rvol_rising_streak", "rvol_rising_streak"
            ),
            rvolAccel = firstDouble(
                obj, "candidate_rvol_accel", "rvol_accel"
            ),
            compressionRatio = firstDouble(
                obj, "candidate_compression_ratio", "compression_ratio"
            ),
            rangeExpansion = firstDouble(
                obj, "candidate_range_expansion", "range_expansion"
            ),
            price15mPct = firstDouble(
                obj, "candidate_price_15m_pct", "price_15m_pct"
            ),
            price1hPct = firstDouble(
                obj, "candidate_price_1h_pct", "price_1h_pct"
            ),
            takerBuyRatio = firstDouble(
                obj, "candidate_taker_buy_ratio", "taker_buy_ratio"
            ),
            btc1hPct = firstDouble(
                obj, "candidate_btc_1h_pct", "btc_1h_pct"
            ),
            engineSignalFound = engineFound,
            engineSignalType = firstString(
                obj, "engine_first_signal_type", "engine_signal_type"
            ),
            engineSignalTimestampUtc = firstString(
                obj,
                "engine_first_signal_timestamp_utc",
                "engine_signal_timestamp_utc"
            ),
            relation = relation,
            deltaMinutes = delta,
            leadMinutes = lead,
            engineQualityScore = firstInt(
                obj, "engine_quality_score", "quality_score"
            ),
            engineEntryStatus = firstString(
                obj, "engine_entry_status", "entry_status"
            ),
            engineMoveStage = firstString(
                obj, "engine_move_stage", "move_stage"
            ),
            fusionScore = firstInt(
                obj, "engine_v32_fusion_score", "fusion_score"
            ),
            fusionGrade = firstString(
                obj, "engine_v32_fusion_grade", "fusion_grade"
            ),
            fusionAllowed = firstBoolean(
                obj, "engine_v32_allowed", "fusion_allowed"
            ),
            fusionReason = firstString(
                obj, "engine_v32_decision_reason", "fusion_reason"
            ),
            phaseBefore = firstString(
                obj, "engine_v32_phase_before", "phase_before"
            ),
            phaseAfter = firstString(
                obj, "engine_v32_phase_after", "phase_after"
            ),
            comparisonStatus = firstString(
                obj, "comparison_status", "status"
            ) ?: if (engineFound) "COMPLETE" else "PENDING",
            outcomes = listOf(5, 10, 15, 30).map { horizon ->
                ShadowOutcomeUi(
                    horizonMinutes = horizon,
                    changePct = firstDouble(
                        obj, "engine_outcome_${horizon}m_change_pct"
                    ),
                    maxGainPct = firstDouble(
                        obj, "engine_outcome_${horizon}m_max_gain_pct"
                    ),
                    maxDrawdownPct = firstDouble(
                        obj, "engine_outcome_${horizon}m_max_drawdown_pct"
                    )
                )
            }
        )
    }

    private fun relationKind(item: ShadowComparisonUi): String {
        val rel = item.relation.uppercase()
        if ("CANDIDATE_BEFORE" in rel || "ENGINE_AFTER_CANDIDATE" in rel) {
            return "CANDIDATE_BEFORE_ENGINE"
        }
        if ("ENGINE_BEFORE" in rel) {
            return "ENGINE_BEFORE_CANDIDATE"
        }
        if ("SAME" in rel) {
            return "SAME_TIMESTAMP"
        }
        return rel
    }

    private fun fetchObject(baseUrl: String, path: String): JSONObject {
        val connection = URL(baseUrl + path).openConnection() as HttpURLConnection
        connection.requestMethod = "GET"
        connection.connectTimeout = 8_000
        connection.readTimeout = 12_000
        connection.setRequestProperty("Accept", "application/json")
        connection.setRequestProperty("User-Agent", "PumpHunterAndroid/3.2")

        try {
            val code = connection.responseCode
            val stream = if (code in 200..299) connection.inputStream else connection.errorStream
            val body = stream?.bufferedReader()?.use { it.readText() }.orEmpty()

            if (code !in 200..299) {
                throw IllegalStateException("HTTP $code $path: ${body.take(180)}")
            }
            return JSONObject(body)
        } finally {
            connection.disconnect()
        }
    }

    private fun firstString(obj: JSONObject, vararg keys: String): String? {
        keys.forEach { key ->
            if (obj.has(key) && !obj.isNull(key)) {
                val value = obj.optString(key)
                if (value.isNotBlank() && value != "null") return value
            }
        }
        return null
    }

    private fun firstDouble(obj: JSONObject, vararg keys: String): Double? {
        keys.forEach { key ->
            if (!obj.has(key) || obj.isNull(key)) return@forEach
            when (val raw = obj.opt(key)) {
                is Number -> return raw.toDouble()
                is String -> raw.toDoubleOrNull()?.let { return it }
            }
        }
        return null
    }

    private fun firstInt(obj: JSONObject, vararg keys: String): Int? =
        firstDouble(obj, *keys)?.toInt()

    private fun firstLong(obj: JSONObject, vararg keys: String): Long? =
        firstDouble(obj, *keys)?.toLong()

    private fun firstBoolean(obj: JSONObject, vararg keys: String): Boolean? {
        keys.forEach { key ->
            if (!obj.has(key) || obj.isNull(key)) return@forEach
            when (val raw = obj.opt(key)) {
                is Boolean -> return raw
                is Number -> return raw.toInt() != 0
                is String -> when {
                    raw.equals("true", true) -> return true
                    raw.equals("false", true) -> return false
                    raw == "1" -> return true
                    raw == "0" -> return false
                }
            }
        }
        return null
    }

    private fun normalizeBaseUrl(value: String): String {
        val clean = value.trim().trimEnd('/')
        if (clean.isBlank()) return ""
        return if (clean.startsWith("http://") || clean.startsWith("https://")) clean
        else "http://$clean"
    }
}
