package com.revolutscanner

import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.Path
import android.view.View
import kotlin.math.max
import kotlin.math.min

class PumpChartView(context: Context) : View(context) {

    private var points: List<PumpHistoryPoint> = emptyList()

    private val linePaint = Paint().apply {
        color = Color.rgb(0, 220, 140)
        strokeWidth = 5f
        style = Paint.Style.STROKE
        isAntiAlias = true
    }

    private val gridPaint = Paint().apply {
        color = Color.rgb(55, 60, 68)
        strokeWidth = 1f
        style = Paint.Style.STROKE
    }

    private val textPaint = Paint().apply {
        color = Color.LTGRAY
        textSize = 30f
        isAntiAlias = true
    }

    private val startPaint = Paint().apply {
        color = Color.rgb(255, 170, 0)
        strokeWidth = 3f
        style = Paint.Style.STROKE
    }

    fun setHistory(history: List<PumpHistoryPoint>) {
        points = history
        invalidate()
    }

    override fun onDraw(canvas: Canvas) {
        super.onDraw(canvas)

        canvas.drawColor(
            Color.rgb(15, 17, 21)
        )

        if (points.size < 2) {
            canvas.drawText(
                "Zbieranie danych do wykresu...",
                30f,
                height / 2f,
                textPaint
            )
            return
        }

        val paddingLeft = 35f
        val paddingRight = 25f
        val paddingTop = 40f
        val paddingBottom = 50f

        val chartWidth =
            width - paddingLeft - paddingRight

        val chartHeight =
            height - paddingTop - paddingBottom

        drawGrid(
            canvas,
            paddingLeft,
            paddingTop,
            chartWidth,
            chartHeight
        )

        var minPrice = Double.MAX_VALUE
        var maxPrice = -Double.MAX_VALUE

        for (point in points) {
            minPrice = min(
                minPrice,
                point.price
            )

            maxPrice = max(
                maxPrice,
                point.price
            )
        }

        if (maxPrice == minPrice) {
            maxPrice += 0.000001
            minPrice -= 0.000001
        }

        val priceRange =
            maxPrice - minPrice

        val path = Path()

        points.forEachIndexed { index, point ->

            val x =
                paddingLeft +
                    (
                        index.toFloat() /
                            (points.size - 1).toFloat()
                        ) * chartWidth

            val normalized =
                (
                    (point.price - minPrice) /
                        priceRange
                    ).toFloat()

            val y =
                paddingTop +
                    chartHeight -
                    (normalized * chartHeight)

            if (index == 0) {
                path.moveTo(x, y)
            } else {
                path.lineTo(x, y)
            }
        }

        canvas.drawPath(
            path,
            linePaint
        )

        // Linia ceny z momentu wykrycia pumpa
        val detectionPrice =
            points.first().price

        val detectionNormalized =
            (
                (detectionPrice - minPrice) /
                    priceRange
                ).toFloat()

        val detectionY =
            paddingTop +
                chartHeight -
                (
                    detectionNormalized *
                        chartHeight
                    )

        canvas.drawLine(
            paddingLeft,
            detectionY,
            paddingLeft + chartWidth,
            detectionY,
            startPaint
        )

        canvas.drawText(
            "START",
            paddingLeft + 10f,
            detectionY - 10f,
            textPaint
        )

        canvas.drawText(
            "MAX ${formatPrice(maxPrice)}",
            paddingLeft,
            30f,
            textPaint
        )

        canvas.drawText(
            "MIN ${formatPrice(minPrice)}",
            paddingLeft,
            height - 10f,
            textPaint
        )
    }

    private fun drawGrid(
        canvas: Canvas,
        left: Float,
        top: Float,
        chartWidth: Float,
        chartHeight: Float
    ) {

        val horizontalLines = 4
        val verticalLines = 6

        for (i in 0..horizontalLines) {

            val y =
                top +
                    (
                        chartHeight /
                            horizontalLines
                        ) * i

            canvas.drawLine(
                left,
                y,
                left + chartWidth,
                y,
                gridPaint
            )
        }

        for (i in 0..verticalLines) {

            val x =
                left +
                    (
                        chartWidth /
                            verticalLines
                        ) * i

            canvas.drawLine(
                x,
                top,
                x,
                top + chartHeight,
                gridPaint
            )
        }
    }

    private fun formatPrice(
        price: Double
    ): String {

        return when {
            price >= 1000 ->
                "%.2f".format(price)

            price >= 1 ->
                "%.4f".format(price)

            else ->
                "%.8f".format(price)
        }
    }
}
