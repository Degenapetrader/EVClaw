use std::collections::{HashMap, VecDeque};
use std::env;
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

const PYTH_WS_ENDPOINTS: [&str; 3] = [
    "wss://pyth-lazer-0.dourolabs.app/v1/stream",
    "wss://pyth-lazer-1.dourolabs.app/v1/stream",
    "wss://pyth-lazer-2.dourolabs.app/v1/stream",
];
const PYTH_SYMBOLS_URL: &str = "https://pyth-lazer-proxy-3.dourolabs.app/v1/symbols";
const HL_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

#[derive(Debug, Parser)]
#[command(name = "pyth-paper")]
#[command(about = "Paper-test Pyth fair value against Hyperliquid xyz mids")]
struct Cli {
    #[arg(long, default_value = "xyz:NVDA")]
    symbol: String,
    #[arg(long, default_value_t = 180)]
    duration_secs: u64,
    #[arg(long, default_value = "fixed_rate@50ms")]
    pyth_channel: String,
    #[arg(long)]
    pyth_api_key: Option<String>,
    #[arg(long, default_value_t = 20_000)]
    basis_window_ms: u64,
    #[arg(long, default_value_t = 50)]
    min_basis_samples: usize,
    #[arg(long, default_value_t = 2.0)]
    entry_bps: f64,
    #[arg(long, default_value_t = 1.5)]
    entry_z: f64,
    #[arg(long, default_value_t = 0.6)]
    take_profit_bps: f64,
    #[arg(long, default_value_t = 3.0)]
    stop_loss_bps: f64,
    #[arg(long, default_value_t = 3_000)]
    max_hold_ms: u64,
    #[arg(long, default_value_t = 1_000)]
    fair_stale_ms: u64,
    #[arg(long, default_value_t = 1_000.0)]
    size_usd: f64,
    #[arg(long, default_value_t = 2.5)]
    fee_bps: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct PythSymbolRow {
    pyth_lazer_id: u64,
    name: String,
    symbol: String,
    asset_type: String,
    exponent: i32,
    state: String,
}

#[derive(Debug, Clone)]
struct PythFeedMeta {
    feed_id: u64,
    exponent: i32,
}

#[derive(Debug, Clone, Copy)]
struct LatestPyth {
    fair_price: f64,
    recv_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct LatestHl {
    bid: f64,
    ask: f64,
    mid: f64,
    recv_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Long,
    Short,
}

#[derive(Debug, Clone)]
struct PaperPosition {
    side: Side,
    qty: f64,
    entry_ms: u64,
    entry_price: f64,
    entry_basis_usd: f64,
    entry_residual_bps: f64,
}

#[derive(Debug, Clone)]
struct TradeRecord {
    side: Side,
    entry_ms: u64,
    exit_ms: u64,
    entry_price: f64,
    exit_price: f64,
    entry_residual_bps: f64,
    exit_residual_bps: f64,
    gross_pnl_usd: f64,
    net_pnl_usd: f64,
    hold_ms: u64,
    reason: &'static str,
}

#[derive(Debug)]
enum Event {
    Pyth {
        source_timestamp_us: u64,
        recv_ms: u64,
        price: f64,
    },
    Hl {
        recv_ms: u64,
        bid: f64,
        ask: f64,
    },
}

#[derive(Debug)]
struct RollingWindow {
    window_ms: u64,
    values: VecDeque<(u64, f64)>,
    sum: f64,
    sum_sq: f64,
}

impl RollingWindow {
    fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            values: VecDeque::new(),
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    fn push(&mut self, now_ms: u64, value: f64) {
        self.values.push_back((now_ms, value));
        self.sum += value;
        self.sum_sq += value * value;
        self.trim(now_ms);
    }

    fn trim(&mut self, now_ms: u64) {
        while let Some((ts, value)) = self.values.front().copied() {
            if now_ms.saturating_sub(ts) <= self.window_ms {
                break;
            }
            self.values.pop_front();
            self.sum -= value;
            self.sum_sq -= value * value;
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn mean(&self) -> Option<f64> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.sum / self.values.len() as f64)
        }
    }

    fn stddev(&self) -> Option<f64> {
        let n = self.values.len();
        if n < 2 {
            return None;
        }
        let mean = self.sum / n as f64;
        let variance = (self.sum_sq / n as f64) - (mean * mean);
        Some(variance.max(0.0).sqrt())
    }
}

#[derive(Debug)]
struct Simulator {
    cfg: Cli,
    basis_window: RollingWindow,
    latest_pyth: Option<LatestPyth>,
    latest_hl: Option<LatestHl>,
    position: Option<PaperPosition>,
    trades: Vec<TradeRecord>,
    seen_pyth_timestamps: HashMap<u64, u64>,
    residual_samples: usize,
    suppressed_stale_events: u64,
}

impl Simulator {
    fn new(cfg: Cli) -> Self {
        Self {
            basis_window: RollingWindow::new(cfg.basis_window_ms),
            latest_pyth: None,
            latest_hl: None,
            position: None,
            trades: Vec::new(),
            seen_pyth_timestamps: HashMap::new(),
            residual_samples: 0,
            suppressed_stale_events: 0,
            cfg,
        }
    }

    fn on_pyth(&mut self, source_timestamp_us: u64, recv_ms: u64, price: f64) {
        if let Some(existing) = self.seen_pyth_timestamps.get(&source_timestamp_us).copied() {
            if recv_ms >= existing {
                return;
            }
        }
        self.seen_pyth_timestamps
            .insert(source_timestamp_us, recv_ms);
        self.latest_pyth = Some(LatestPyth {
            fair_price: price,
            recv_ms,
        });
    }

    fn on_hl(&mut self, recv_ms: u64, bid: f64, ask: f64) {
        let mid = (bid + ask) / 2.0;
        self.latest_hl = Some(LatestHl {
            bid,
            ask,
            mid,
            recv_ms,
        });
        let Some(pyth) = self.latest_pyth else {
            return;
        };
        if recv_ms.saturating_sub(pyth.recv_ms) > self.cfg.fair_stale_ms {
            self.suppressed_stale_events += 1;
            if self.position.is_some() {
                self.exit_position(recv_ms, bid, ask, pyth.fair_price, 0.0, "fair_stale");
            }
            return;
        }

        let basis = mid - pyth.fair_price;
        self.basis_window.push(recv_ms, basis);
        if self.basis_window.len() < self.cfg.min_basis_samples {
            return;
        }
        let Some(mean_basis) = self.basis_window.mean() else {
            return;
        };
        let Some(std_basis) = self.basis_window.stddev() else {
            return;
        };
        if std_basis <= 1e-9 {
            return;
        }

        let residual_usd = basis - mean_basis;
        let residual_bps = residual_usd / pyth.fair_price * 10_000.0;
        let zscore = residual_usd / std_basis;
        self.residual_samples += 1;

        if self.position.is_some() {
            self.maybe_exit(recv_ms, bid, ask, pyth.fair_price, residual_bps);
        } else {
            self.maybe_enter(
                recv_ms,
                bid,
                ask,
                pyth.fair_price,
                mean_basis,
                residual_bps,
                zscore,
            );
        }
    }

    fn maybe_enter(
        &mut self,
        now_ms: u64,
        bid: f64,
        ask: f64,
        _fair_price: f64,
        basis_mean_usd: f64,
        residual_bps: f64,
        zscore: f64,
    ) {
        if residual_bps <= -self.cfg.entry_bps && zscore <= -self.cfg.entry_z {
            let qty = self.cfg.size_usd / ask;
            self.position = Some(PaperPosition {
                side: Side::Long,
                qty,
                entry_ms: now_ms,
                entry_price: ask,
                entry_basis_usd: basis_mean_usd,
                entry_residual_bps: residual_bps,
            });
        } else if residual_bps >= self.cfg.entry_bps && zscore >= self.cfg.entry_z {
            let qty = self.cfg.size_usd / bid;
            self.position = Some(PaperPosition {
                side: Side::Short,
                qty,
                entry_ms: now_ms,
                entry_price: bid,
                entry_basis_usd: basis_mean_usd,
                entry_residual_bps: residual_bps,
            });
        }
    }

    fn maybe_exit(&mut self, now_ms: u64, bid: f64, ask: f64, fair_price: f64, residual_bps: f64) {
        let Some(position) = self.position.as_ref() else {
            return;
        };

        let hold_ms = now_ms.saturating_sub(position.entry_ms);
        let reason = match position.side {
            Side::Long if residual_bps >= -self.cfg.take_profit_bps => Some("take_profit"),
            Side::Short if residual_bps <= self.cfg.take_profit_bps => Some("take_profit"),
            Side::Long if residual_bps <= -self.cfg.stop_loss_bps => Some("stop_loss"),
            Side::Short if residual_bps >= self.cfg.stop_loss_bps => Some("stop_loss"),
            _ if hold_ms >= self.cfg.max_hold_ms => Some("inventory_timeout"),
            _ => None,
        };

        if let Some(reason) = reason {
            self.exit_position(now_ms, bid, ask, fair_price, residual_bps, reason);
        }
    }

    fn exit_position(
        &mut self,
        now_ms: u64,
        bid: f64,
        ask: f64,
        _fair_price: f64,
        residual_bps: f64,
        reason: &'static str,
    ) {
        let Some(position) = self.position.take() else {
            return;
        };
        let exit_price = match position.side {
            Side::Long => bid,
            Side::Short => ask,
        };
        let gross = match position.side {
            Side::Long => (exit_price - position.entry_price) * position.qty,
            Side::Short => (position.entry_price - exit_price) * position.qty,
        };
        let entry_fee = position.entry_price * position.qty * self.cfg.fee_bps / 10_000.0;
        let exit_fee = exit_price * position.qty * self.cfg.fee_bps / 10_000.0;
        self.trades.push(TradeRecord {
            side: position.side,
            entry_ms: position.entry_ms,
            exit_ms: now_ms,
            entry_price: position.entry_price,
            exit_price,
            entry_residual_bps: position.entry_residual_bps,
            exit_residual_bps: residual_bps,
            gross_pnl_usd: gross,
            net_pnl_usd: gross - entry_fee - exit_fee,
            hold_ms: now_ms.saturating_sub(position.entry_ms),
            reason,
        });
    }
}

async fn resolve_pyth_feed(client: &Client, symbol: &str) -> Result<PythFeedMeta> {
    let ticker = symbol
        .strip_prefix("xyz:")
        .ok_or_else(|| anyhow!("symbol must be a HIP-3 symbol like xyz:NVDA"))?;
    let rows: Vec<PythSymbolRow> = client
        .get(PYTH_SYMBOLS_URL)
        .send()
        .await
        .context("failed to fetch Pyth symbols list")?
        .error_for_status()
        .context("Pyth symbols list request failed")?
        .json()
        .await
        .context("failed to decode Pyth symbols list")?;
    let expected_symbol = format!("Equity.US.{ticker}/USD");
    let row = rows
        .into_iter()
        .find(|row| {
            row.asset_type == "equity"
                && row.state == "stable"
                && (row.symbol == expected_symbol || row.name == ticker)
        })
        .ok_or_else(|| anyhow!("no stable Pyth feed found for {symbol}"))?;
    Ok(PythFeedMeta {
        feed_id: row.pyth_lazer_id,
        exponent: row.exponent,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn scale_pyth_price(raw_price: i64, exponent: i32) -> f64 {
    (raw_price as f64) * 10f64.powi(exponent)
}

async fn spawn_pyth_task(
    tx: mpsc::UnboundedSender<Event>,
    api_key: String,
    endpoint: &'static str,
    feed_meta: PythFeedMeta,
    channel: String,
    subscription_id: u64,
) {
    let mut request = match endpoint.into_client_request() {
        Ok(req) => req,
        Err(_) => return,
    };
    let header_value = match HeaderValue::from_str(&format!("Bearer {api_key}")) {
        Ok(value) => value,
        Err(_) => return,
    };
    request.headers_mut().insert("Authorization", header_value);

    let Ok((mut ws, _)) = connect_async(request).await else {
        return;
    };

    let subscribe = json!({
        "type": "subscribe",
        "subscriptionId": subscription_id,
        "priceFeedIds": [feed_meta.feed_id],
        "properties": ["price", "feedUpdateTimestamp"],
        "formats": [],
        "channel": channel,
    });
    if ws.send(Message::Text(subscribe.to_string())).await.is_err() {
        return;
    }

    while let Some(message) = ws.next().await {
        let Ok(Message::Text(text)) = message else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) != Some("streamUpdated") {
            continue;
        }
        let Some(price_feeds) = value
            .get("parsed")
            .and_then(|parsed| parsed.get("priceFeeds"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for feed in price_feeds {
            let Some(source_timestamp_us) = feed.get("feedUpdateTimestamp").and_then(Value::as_u64)
            else {
                continue;
            };
            let Some(raw_price) = feed.get("price").and_then(Value::as_str) else {
                continue;
            };
            let Ok(raw_price) = i64::from_str(raw_price) else {
                continue;
            };
            let _ = tx.send(Event::Pyth {
                source_timestamp_us,
                recv_ms: now_ms(),
                price: scale_pyth_price(raw_price, feed_meta.exponent),
            });
        }
    }
}

async fn spawn_hl_task(tx: mpsc::UnboundedSender<Event>, symbol: String) {
    let Ok((mut ws, _)) = connect_async(HL_WS_URL).await else {
        return;
    };

    let subscribe = json!({
        "method": "subscribe",
        "subscription": {
            "type": "l2Book",
            "coin": symbol,
        }
    });
    if ws.send(Message::Text(subscribe.to_string())).await.is_err() {
        return;
    }

    while let Some(message) = ws.next().await {
        let Ok(Message::Text(text)) = message else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        if value.get("channel").and_then(Value::as_str) != Some("l2Book") {
            continue;
        }
        let Some(data) = value.get("data") else {
            continue;
        };
        let Some(levels) = data.get("levels").and_then(Value::as_array) else {
            continue;
        };
        let Some(best_bid) = levels
            .first()
            .and_then(Value::as_array)
            .and_then(|side| side.first())
        else {
            continue;
        };
        let Some(best_ask) = levels
            .get(1)
            .and_then(Value::as_array)
            .and_then(|side| side.first())
        else {
            continue;
        };
        let Some(bid) = best_bid
            .get("px")
            .and_then(Value::as_str)
            .and_then(|px| px.parse::<f64>().ok())
        else {
            continue;
        };
        let Some(ask) = best_ask
            .get("px")
            .and_then(Value::as_str)
            .and_then(|px| px.parse::<f64>().ok())
        else {
            continue;
        };
        let _ = tx.send(Event::Hl {
            recv_ms: now_ms(),
            bid,
            ask,
        });
    }
}

fn count_reason(trades: &[TradeRecord], reason: &str) -> usize {
    trades.iter().filter(|trade| trade.reason == reason).count()
}

fn format_side(side: Side) -> &'static str {
    match side {
        Side::Long => "long",
        Side::Short => "short",
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let api_key = cli
        .pyth_api_key
        .clone()
        .or_else(|| env::var("PYTH_PRO_API_KEY").ok())
        .ok_or_else(|| {
            anyhow!("missing Pyth API key; set PYTH_PRO_API_KEY or pass --pyth-api-key")
        })?;

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to build reqwest client")?;
    let feed_meta = resolve_pyth_feed(&client, &cli.symbol).await?;

    let mut simulator = Simulator::new(cli);
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let (tx, mut rx) = mpsc::unbounded_channel();

    for (idx, endpoint) in PYTH_WS_ENDPOINTS.into_iter().enumerate() {
        tokio::spawn(spawn_pyth_task(
            tx.clone(),
            api_key.clone(),
            endpoint,
            feed_meta.clone(),
            simulator.cfg.pyth_channel.clone(),
            idx as u64 + 1,
        ));
    }
    tokio::spawn(spawn_hl_task(tx.clone(), simulator.cfg.symbol.clone()));
    drop(tx);

    let started = Instant::now();
    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                match event {
                    Event::Pyth { source_timestamp_us, recv_ms, price } => simulator.on_pyth(source_timestamp_us, recv_ms, price),
                    Event::Hl { recv_ms, bid, ask } => simulator.on_hl(recv_ms, bid, ask),
                }
            }
            _ = interval.tick() => {
                if started.elapsed() >= Duration::from_secs(simulator.cfg.duration_secs) {
                    break;
                }
            }
        }
    }

    if let (Some(position), Some(hl), Some(pyth)) = (
        simulator.position.clone(),
        simulator.latest_hl,
        simulator.latest_pyth,
    ) {
        let residual_bps =
            ((hl.mid - pyth.fair_price) - position.entry_basis_usd) / pyth.fair_price * 10_000.0;
        simulator.exit_position(
            hl.recv_ms,
            hl.bid,
            hl.ask,
            pyth.fair_price,
            residual_bps,
            "session_end",
        );
    }

    let total_gross: f64 = simulator
        .trades
        .iter()
        .map(|trade| trade.gross_pnl_usd)
        .sum();
    let total_net: f64 = simulator.trades.iter().map(|trade| trade.net_pnl_usd).sum();
    let wins = simulator
        .trades
        .iter()
        .filter(|trade| trade.net_pnl_usd > 0.0)
        .count();
    let avg_hold_ms = if simulator.trades.is_empty() {
        0.0
    } else {
        simulator
            .trades
            .iter()
            .map(|trade| trade.hold_ms as f64)
            .sum::<f64>()
            / simulator.trades.len() as f64
    };

    let summary = json!({
        "symbol": simulator.cfg.symbol,
        "feed_id": feed_meta.feed_id,
        "duration_secs": simulator.cfg.duration_secs,
        "strategy": {
            "basis_window_ms": simulator.cfg.basis_window_ms,
            "min_basis_samples": simulator.cfg.min_basis_samples,
            "entry_bps": simulator.cfg.entry_bps,
            "entry_z": simulator.cfg.entry_z,
            "take_profit_bps": simulator.cfg.take_profit_bps,
            "stop_loss_bps": simulator.cfg.stop_loss_bps,
            "max_hold_ms": simulator.cfg.max_hold_ms,
            "fair_stale_ms": simulator.cfg.fair_stale_ms,
            "size_usd": simulator.cfg.size_usd,
            "fee_bps": simulator.cfg.fee_bps,
            "pyth_channel": simulator.cfg.pyth_channel,
        },
        "market_data": {
            "basis_samples": simulator.basis_window.len(),
            "residual_samples": simulator.residual_samples,
            "deduped_pyth_updates": simulator.seen_pyth_timestamps.len(),
            "suppressed_stale_events": simulator.suppressed_stale_events,
        },
        "paper_results": {
            "trade_count": simulator.trades.len(),
            "win_rate_pct": if simulator.trades.is_empty() { 0.0 } else { wins as f64 * 100.0 / simulator.trades.len() as f64 },
            "gross_pnl_usd": total_gross,
            "net_pnl_usd": total_net,
            "avg_hold_ms": avg_hold_ms,
            "take_profit_count": count_reason(&simulator.trades, "take_profit"),
            "stop_loss_count": count_reason(&simulator.trades, "stop_loss"),
            "inventory_timeout_count": count_reason(&simulator.trades, "inventory_timeout"),
            "fair_stale_count": count_reason(&simulator.trades, "fair_stale"),
            "session_end_count": count_reason(&simulator.trades, "session_end"),
        },
        "trades": simulator.trades.iter().map(|trade| json!({
            "side": format_side(trade.side),
            "entry_ms": trade.entry_ms,
            "exit_ms": trade.exit_ms,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "entry_residual_bps": trade.entry_residual_bps,
            "exit_residual_bps": trade.exit_residual_bps,
            "gross_pnl_usd": trade.gross_pnl_usd,
            "net_pnl_usd": trade.net_pnl_usd,
            "hold_ms": trade.hold_ms,
            "reason": trade.reason,
        })).collect::<Vec<_>>(),
    });

    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
