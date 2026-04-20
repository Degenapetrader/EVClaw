use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::Value;

use crate::config::Config;
use crate::hyperliquid::InfoClient;
use crate::types::{AtrCheckpointData, AtrData};

const ATR_CACHE_TTL_SECS: u64 = 300;
const ENTRY_GUARD_4H_LOOKBACK: usize = 4;
const ENTRY_GUARD_8H_LOOKBACK: usize = 8;
const ENTRY_GUARD_12H_LOOKBACK: usize = 12;
const ENTRY_GUARD_MIN_CANDLES: usize = ENTRY_GUARD_12H_LOOKBACK + 1;
const VOLATILITY_TIERS: &[(&str, f64, f64, f64)] = &[
    ("STABLE", 0.5, 1.0, 1.4),
    ("LOW", 0.8, 1.4, 1.75),
    ("MEDIUM", 1.5, 1.75, 2.1),
    ("HIGH", 3.0, 2.1, 2.45),
    ("EXTREME", f64::INFINITY, 2.45, 2.8),
];

#[derive(Debug, Clone, Copy)]
struct CandlePoint {
    high: f64,
    low: f64,
    close: f64,
}

pub struct AtrClient {
    http: Client,
    binance_klines_url: String,
    atr_period: usize,
    atr_interval: String,
    cache: Mutex<HashMap<String, (AtrData, Instant)>>,
    checkpoint_cache: Mutex<HashMap<String, (AtrCheckpointData, Instant)>>,
}

impl AtrClient {
    pub fn new(cfg: &Config) -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .timeout(std::time::Duration::from_secs(cfg.request_timeout_secs))
                .build()?,
            binance_klines_url: cfg.binance_klines_url.clone(),
            atr_period: cfg.atr_period,
            atr_interval: cfg.atr_interval.clone(),
            cache: Mutex::new(HashMap::new()),
            checkpoint_cache: Mutex::new(HashMap::new()),
        })
    }

    pub async fn fetch(&self, info_client: &InfoClient, symbol: &str) -> Result<AtrData> {
        if let Some(cached) = self.get_cached(symbol) {
            return Ok(cached);
        }

        let (atr_data, checkpoint_data) = match self.fetch_binance_points(symbol).await {
            Ok(points) => analyze_points(&points, self.atr_period, symbol)?,
            Err(_) => {
                let points = self.fetch_hyperliquid_points(info_client, symbol).await?;
                analyze_points(&points, self.atr_period, symbol)?
            }
        };
        self.store_cached(symbol, atr_data.clone());
        self.store_checkpoint_data(symbol, checkpoint_data);
        Ok(atr_data)
    }

    pub async fn fetch_checkpoint_data(
        &self,
        info_client: &InfoClient,
        symbol: &str,
    ) -> Result<AtrCheckpointData> {
        if let Some(cached) = self.get_cached_checkpoint(symbol) {
            return Ok(cached);
        }

        let (atr_data, checkpoint_data) = match self.fetch_binance_points(symbol).await {
            Ok(points) => analyze_points(&points, self.atr_period, symbol)?,
            Err(_) => {
                let points = self.fetch_hyperliquid_points(info_client, symbol).await?;
                analyze_points(&points, self.atr_period, symbol)?
            }
        };
        self.store_cached(symbol, atr_data);
        self.store_checkpoint_data(symbol, checkpoint_data.clone());
        Ok(checkpoint_data)
    }

    fn get_cached(&self, symbol: &str) -> Option<AtrData> {
        let cache = self.cache.lock().ok()?;
        let (data, fetched_at) = cache.get(symbol)?;
        if fetched_at.elapsed() <= Duration::from_secs(ATR_CACHE_TTL_SECS) {
            Some(data.clone())
        } else {
            None
        }
    }

    fn store_cached(&self, symbol: &str, data: AtrData) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(symbol.to_string(), (data, Instant::now()));
        }
    }

    fn get_cached_checkpoint(&self, symbol: &str) -> Option<AtrCheckpointData> {
        let cache = self.checkpoint_cache.lock().ok()?;
        let (data, fetched_at) = cache.get(symbol)?;
        if fetched_at.elapsed() <= Duration::from_secs(ATR_CACHE_TTL_SECS) {
            Some(data.clone())
        } else {
            None
        }
    }

    fn store_checkpoint_data(&self, symbol: &str, data: AtrCheckpointData) {
        if let Ok(mut cache) = self.checkpoint_cache.lock() {
            cache.insert(symbol.to_string(), (data, Instant::now()));
        }
    }

    async fn fetch_binance_points(&self, symbol: &str) -> Result<Vec<CandlePoint>> {
        let binance_symbol = to_binance_symbol(symbol);
        let data: Vec<Vec<Value>> = self
            .http
            .get(&self.binance_klines_url)
            .query(&[
                ("symbol", binance_symbol.as_str()),
                ("interval", self.atr_interval.as_str()),
                ("limit", &self.kline_limit().to_string()),
            ])
            .send()
            .await?
            .json()
            .await?;
        if data.len() < required_candle_count(self.atr_period) {
            return Err(anyhow!("insufficient Binance candles for {}", symbol));
        }
        Ok(data
            .into_iter()
            .map(|row| CandlePoint {
                high: row.get(2).and_then(value_to_f64).unwrap_or(0.0),
                low: row.get(3).and_then(value_to_f64).unwrap_or(0.0),
                close: row.get(4).and_then(value_to_f64).unwrap_or(0.0),
            })
            .collect())
    }

    async fn fetch_hyperliquid_points(
        &self,
        info_client: &InfoClient,
        symbol: &str,
    ) -> Result<Vec<CandlePoint>> {
        let candles = info_client
            .fetch_hl_candles(symbol, &self.atr_interval)
            .await?;
        if candles.len() < required_candle_count(self.atr_period) {
            return Err(anyhow!("insufficient Hyperliquid candles for {}", symbol));
        }
        Ok(candles
            .into_iter()
            .map(|row| CandlePoint {
                high: row.get("h").and_then(value_to_f64).unwrap_or(0.0),
                low: row.get("l").and_then(value_to_f64).unwrap_or(0.0),
                close: row.get("c").and_then(value_to_f64).unwrap_or(0.0),
            })
            .collect())
    }

    fn kline_limit(&self) -> usize {
        (self.atr_period + 5).max(required_candle_count(self.atr_period))
    }
}

fn analyze_points(
    points: &[CandlePoint],
    atr_period: usize,
    symbol: &str,
) -> Result<(AtrData, AtrCheckpointData)> {
    if points.len() < required_candle_count(atr_period) {
        return Err(anyhow!("not enough candles for {}", symbol));
    }
    let atr = atr_from_points(points, atr_period)?;
    let close = points.last().map(|point| point.close).unwrap_or(0.0);
    let atr_data = atr_data(atr, close, symbol)?;
    let checkpoint_data = checkpoint_data(atr, points, symbol)?;
    Ok((atr_data, checkpoint_data))
}

fn atr_from_points(points: &[CandlePoint], period: usize) -> Result<f64> {
    if points.len() < period + 1 {
        return Err(anyhow!("not enough values for ATR"));
    }
    let mut trs = Vec::with_capacity(points.len().saturating_sub(1));
    for i in 1..points.len() {
        let high = points[i].high;
        let low = points[i].low;
        let prev_close = points[i - 1].close;
        trs.push(
            (high - low)
                .max((high - prev_close).abs())
                .max((low - prev_close).abs()),
        );
    }
    average_tail(&trs, period)
}

fn checkpoint_data(atr: f64, points: &[CandlePoint], symbol: &str) -> Result<AtrCheckpointData> {
    if points.len() < ENTRY_GUARD_MIN_CANDLES {
        return Err(anyhow!("not enough checkpoint candles for {}", symbol));
    }
    let point_4h = checkpoint_point(points, ENTRY_GUARD_4H_LOOKBACK, symbol)?;
    let point_8h = checkpoint_point(points, ENTRY_GUARD_8H_LOOKBACK, symbol)?;
    let point_12h = checkpoint_point(points, ENTRY_GUARD_12H_LOOKBACK, symbol)?;
    Ok(AtrCheckpointData {
        atr,
        high_4h_ago: point_4h.high,
        low_4h_ago: point_4h.low,
        high_8h_ago: point_8h.high,
        low_8h_ago: point_8h.low,
        high_12h_ago: point_12h.high,
        low_12h_ago: point_12h.low,
    })
}

fn checkpoint_point(
    points: &[CandlePoint],
    lookback_hours: usize,
    symbol: &str,
) -> Result<CandlePoint> {
    let idx = points
        .len()
        .checked_sub(lookback_hours + 1)
        .ok_or_else(|| anyhow!("not enough checkpoint candles for {}", symbol))?;
    let point = points[idx];
    if point.high <= 0.0 || point.low <= 0.0 || point.high < point.low {
        return Err(anyhow!("invalid checkpoint candle for {}", symbol));
    }
    Ok(point)
}

fn required_candle_count(atr_period: usize) -> usize {
    (atr_period + 1).max(ENTRY_GUARD_MIN_CANDLES)
}

fn atr_data(atr: f64, close: f64, symbol: &str) -> Result<AtrData> {
    if close <= 0.0 || atr <= 0.0 {
        return Err(anyhow!("invalid ATR data for {}", symbol));
    }
    let atr_pct = (atr / close) * 100.0;
    let (tier, min_mult, max_mult) = classify_volatility(atr_pct);
    Ok(AtrData {
        atr,
        atr_pct,
        volatility_tier: tier.to_string(),
        min_mult,
        max_mult,
    })
}

fn classify_volatility(atr_pct: f64) -> (&'static str, f64, f64) {
    for (tier, max_atr, min_mult, max_mult) in VOLATILITY_TIERS {
        if atr_pct <= *max_atr {
            return (tier, *min_mult, *max_mult);
        }
    }
    ("EXTREME", 2.45, 2.8)
}

fn average_tail(values: &[f64], period: usize) -> Result<f64> {
    if values.len() < period {
        return Err(anyhow!("not enough values for ATR"));
    }
    let tail = &values[values.len() - period..];
    Ok(tail.iter().sum::<f64>() / tail.len() as f64)
}

fn to_binance_symbol(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    if let Some(rest) = upper.strip_prefix('K') {
        if !rest.is_empty() {
            return format!("1000{}USDT", rest);
        }
    }
    format!("{}USDT", upper)
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}
