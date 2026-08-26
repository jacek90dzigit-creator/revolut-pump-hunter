package com.revolutscanner

import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

data class RevolutTicker(
    val symbol: String,
    val lastPrice: Double,
    val bidPrice: Double,
    val askPrice: Double
)

object RevolutApi {

    private const val BASE_URL =
        "https://revx.revolut.com/api/1.0/public/tickers?region=EEA"

    fun getTickers(): List<RevolutTicker> {

        val url = URL(BASE_URL)
        val connection = url.openConnection() as HttpURLConnection

        connection.requestMethod = "GET"
        connection.connectTimeout = 10_000
        connection.readTimeout = 10_000

        try {
            val responseCode = connection.responseCode

            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw Exception("Błąd Revolut API: HTTP $responseCode")
            }

            val response = connection.inputStream
                .bufferedReader()
                .use { it.readText() }

            val json = JSONObject(response)
            val data = json.getJSONArray("data")

            val result = mutableListOf<RevolutTicker>()

            for (i in 0 until data.length()) {

                val item = data.getJSONObject(i)

                val symbol = item.optString("symbol")

                val lastPrice =
                    item.optString("last_price", "0")
                        .toDoubleOrNull() ?: 0.0

                val bidPrice =
                    item.optString("bid_price", "0")
                        .toDoubleOrNull() ?: 0.0

                val askPrice =
                    item.optString("ask_price", "0")
                        .toDoubleOrNull() ?: 0.0

                if (symbol.isNotBlank() && lastPrice > 0) {
                    result.add(
                        RevolutTicker(
                            symbol = symbol,
                            lastPrice = lastPrice,
                            bidPrice = bidPrice,
                            askPrice = askPrice
                        )
                    )
                }
            }

            return result

        } finally {
            connection.disconnect()
        }
    }
}
