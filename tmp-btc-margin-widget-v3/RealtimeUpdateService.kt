package com.lactoone.marginwidget

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class RealtimeUpdateService : Service() {
    private val handler = Handler(Looper.getMainLooper())
    private val balanceFetching = AtomicBoolean(false)
    private val priceRestFetching = AtomicBoolean(false)
    private var receiverRegistered = false
    private var socket: WebSocket? = null
    private var rateLimitBackoffUntil = 0L

    @Volatile private var latestPrice: Double? = null
    @Volatile private var latestPriceAtMs: Long = 0L
    @Volatile private var latestBalanceText: String = "—"

    private val http = OkHttpClient.Builder()
        .pingInterval(20, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .build()

    private val screenReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            when (intent?.action) {
                Intent.ACTION_SCREEN_ON -> enterRealtimeMode()
                Intent.ACTION_SCREEN_OFF -> enterSleepMode()
            }
        }
    }

    private val screenTickRunnable = object : Runnable {
        override fun run() {
            if (!isScreenInteractive()) return

            val now = System.currentTimeMillis()
            val p = latestPrice
            if (p != null) {
                MarginWidgetProvider.renderPrice(
                    applicationContext,
                    MarginWidgetProvider.formatWhole(p)
                )
            }

            if (p == null || now - latestPriceAtMs > PRICE_STALE_MS) {
                updatePriceRestAsync()
            }

            updateBalanceAsync()
            MarginWidgetProvider.renderBalance(applicationContext, latestBalanceText)
            handler.postDelayed(this, SCREEN_ON_INTERVAL_MS)
        }
    }

    private val sleepRefreshRunnable = object : Runnable {
        override fun run() {
            updateSleepSnapshot()
        }
    }

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
        startForeground(NOTIFICATION_ID, buildNotification())
        registerScreenReceiver()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (isScreenInteractive()) enterRealtimeMode() else enterSleepMode()
        if (intent?.action == ACTION_REFRESH_NOW) {
            updatePriceRestAsync(force = true)
            updateBalanceAsync(force = true)
        }
        return START_STICKY
    }

    override fun onDestroy() {
        handler.removeCallbacksAndMessages(null)
        closeSocket()
        if (receiverRegistered) {
            runCatching { unregisterReceiver(screenReceiver) }
            receiverRegistered = false
        }
        http.dispatcher.executorService.shutdown()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun registerScreenReceiver() {
        val filter = IntentFilter().apply {
            addAction(Intent.ACTION_SCREEN_ON)
            addAction(Intent.ACTION_SCREEN_OFF)
        }
        if (Build.VERSION.SDK_INT >= 33) {
            registerReceiver(screenReceiver, filter, RECEIVER_NOT_EXPORTED)
        } else {
            @Suppress("DEPRECATION") registerReceiver(screenReceiver, filter)
        }
        receiverRegistered = true
    }

    private fun enterRealtimeMode() {
        handler.removeCallbacks(sleepRefreshRunnable)
        handler.removeCallbacks(screenTickRunnable)
        connectPriceSocket()
        updatePriceRestAsync(force = true)
        updateBalanceAsync(force = true)
        handler.post(screenTickRunnable)
    }

    private fun enterSleepMode() {
        closeSocket()
        handler.removeCallbacks(screenTickRunnable)
        handler.removeCallbacks(sleepRefreshRunnable)
        handler.post(sleepRefreshRunnable)
    }

    private fun connectPriceSocket() {
        if (socket != null) return
        val request = Request.Builder()
            .url("wss://fstream.binance.com/ws/btcusdt@aggTrade")
            .build()
        socket = http.newWebSocket(request, object : WebSocketListener() {
            override fun onMessage(webSocket: WebSocket, text: String) {
                runCatching {
                    val price = JSONObject(text).getString("p").toDouble()
                    latestPrice = price
                    latestPriceAtMs = System.currentTimeMillis()
                }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                socket = null
                if (isScreenInteractive()) {
                    updatePriceRestAsync(force = true)
                    handler.postDelayed({ connectPriceSocket() }, WS_RECONNECT_MS)
                }
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                socket = null
                if (isScreenInteractive()) {
                    updatePriceRestAsync(force = true)
                    handler.postDelayed({ connectPriceSocket() }, WS_RECONNECT_MS)
                }
            }
        })
    }

    private fun closeSocket() {
        val ws = socket
        socket = null
        runCatching { ws?.close(1000, "screen off") }
    }

    private fun updateBalanceAsync(force: Boolean = false) {
        if (!isScreenInteractive() && !force) return
        val now = System.currentTimeMillis()
        if (now < rateLimitBackoffUntil) return
        if (!balanceFetching.compareAndSet(false, true)) return

        Thread {
            try {
                val creds = SecureStore.load(applicationContext)
                if (creds == null) {
                    latestBalanceText = "—"
                } else {
                    val balance = BinanceClient.fetchMarginBalance(creds.first, creds.second)
                    latestBalanceText = MarginWidgetProvider.formatWhole(balance)
                }
                MarginWidgetProvider.renderBalance(applicationContext, latestBalanceText)
            } catch (_: BinanceClient.RateLimitedException) {
                rateLimitBackoffUntil = System.currentTimeMillis() + RATE_LIMIT_BACKOFF_MS
            } catch (_: Exception) {
            } finally {
                balanceFetching.set(false)
            }
        }.start()
    }

    private fun updatePriceRestAsync(force: Boolean = false) {
        if (!isScreenInteractive() && !force) return
        if (!priceRestFetching.compareAndSet(false, true)) return
        Thread {
            try {
                val price = BinanceClient.fetchBtcPriceRest()
                latestPrice = price
                latestPriceAtMs = System.currentTimeMillis()
                MarginWidgetProvider.renderPrice(
                    applicationContext,
                    MarginWidgetProvider.formatWhole(price)
                )
            } catch (_: Exception) {
            } finally {
                priceRestFetching.set(false)
            }
        }.start()
    }

    private fun updateSleepSnapshot() {
        Thread {
            try {
                val price = BinanceClient.fetchBtcPriceRest()
                latestPrice = price
                latestPriceAtMs = System.currentTimeMillis()

                val creds = SecureStore.load(applicationContext)
                val balance = if (creds == null) null else BinanceClient.fetchMarginBalance(creds.first, creds.second)
                latestBalanceText = balance?.let(MarginWidgetProvider::formatWhole) ?: "—"

                MarginWidgetProvider.renderBoth(
                    applicationContext,
                    MarginWidgetProvider.formatWhole(price),
                    latestBalanceText
                )
            } catch (_: Exception) {
            } finally {
                handler.post {
                    if (!isScreenInteractive()) {
                        handler.postDelayed(sleepRefreshRunnable, SCREEN_OFF_INTERVAL_MS)
                    }
                }
            }
        }.start()
    }

    private fun isScreenInteractive(): Boolean =
        getSystemService(PowerManager::class.java).isInteractive

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val manager = getSystemService(NotificationManager::class.java)
            val channel = NotificationChannel(
                CHANNEL_ID,
                "BTC / Margin 실시간 위젯",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "화면이 켜져 있을 때 BTCUSDT Perp와 Margin Balance를 1초마다 갱신합니다."
                setShowBadge(false)
            }
            manager.createNotificationChannel(channel)
        }
    }

    private fun buildNotification() = NotificationCompat.Builder(this, CHANNEL_ID)
        .setSmallIcon(android.R.drawable.stat_notify_sync_noanim)
        .setContentTitle("BTC / Margin 위젯")
        .setContentText("화면 켜짐: BTC · 잔액 1초 / 화면 꺼짐: 5분")
        .setOngoing(true)
        .setOnlyAlertOnce(true)
        .setPriority(NotificationCompat.PRIORITY_LOW)
        .build()

    companion object {
        const val ACTION_REFRESH_NOW = "com.lactoone.marginwidget.SERVICE_REFRESH_NOW"
        private const val CHANNEL_ID = "btc_margin_realtime"
        private const val NOTIFICATION_ID = 1059
        private const val SCREEN_ON_INTERVAL_MS = 1_000L
        private const val SCREEN_OFF_INTERVAL_MS = 5 * 60_000L
        private const val PRICE_STALE_MS = 3_000L
        private const val WS_RECONNECT_MS = 3_000L
        private const val RATE_LIMIT_BACKOFF_MS = 60_000L

        fun start(context: Context, refreshNow: Boolean = true) {
            val intent = Intent(context, RealtimeUpdateService::class.java).apply {
                action = if (refreshNow) ACTION_REFRESH_NOW else null
            }
            ContextCompat.startForegroundService(context, intent)
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, RealtimeUpdateService::class.java))
        }
    }
}
