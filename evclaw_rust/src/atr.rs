use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::Value;

use crate::config::Config;
use crate::hyperliquid::InfoClient;
use crate::types::AtrData;

const ATR_CACHE_TTL_SECS: u64 = 300;
const VOLATILITY_TIERS: &[(&str, f64, f64, f64)] = &[
    ("STABLE", 0.5, 1.0, 1.4),
    ("LOW", 0.8, 1.4, 1.75),
    ("MEDIUM", 1.5, 1.75, 2.1),
    ("HIGH", 3.0, 2.1, 2.45),
    ("EXTREME", f64::INFINITY, 2.45, 2.8),
];

pub struct AtrClient {
    http: Client,
    binance_klines_url: String,
    atr_period: usize,
    atr_interval: String,
    cache: Mutex<HashMap<String, (AtrData, Instant)>>,
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
        })
    }

    pub async fn fetch(&self, info_client: &InfoClient, symbol: &str) -> Result<AtrData> {
        if let Some(cached) = self.get_cached(symbol) {
            return Ok(cached);
        }

        let data = match self.fetch_binance(symbol).await {
            Ok(data) => data,
            Err(_) => self.fetch_hyperliquid(info_client, symbol).await?,
        };
        self.store_cached(symbol, data.clone());
        Ok(data)
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

    async fn fetch_binance(&self, symbol: &str) -> Result<AtrData> {
        let binance_symbol = to_binance_symbol(symbol);
        let data: Vec<Vec<Value>> = self
            .http
            .get(&self.binance_klines_url)
            .query(&[
                ("symbol", binance_symbol.as_str()),
                ("interval", self.atr_interval.as_str()),
                ("limit", &(self.atr_period + 5).to_string()),
            ])
            .send()
            .await?
            .json()
            .await?;
        if data.len() < self.atr_period + 1 {
            return Err(anyhow!("insufficient Binance candles for {}", symbol));
        }

        let mut trs = Vec::new();
        for i in 1..data.len() {
            let high = value_to_f64(&data[i][2]).unwrap_or(0.0);
            let low = value_to_f64(&data[i][3]).unwrap_or(0.0);
            let prev_close = value_to_f64(&data[i - 1][4]).unwrap_or(0.0);
            trs.push(
                (high - low)
                    .max((high - prev_close).abs())
                    .max((low - prev_close).abs()),
            );
        }
        let atr = average_tail(&trs, self.atr_period)?;
        let close = value_to_f64(
            &data
                .last()
                .and_then(|row| row.get(4))
                .cloned()
                .unwrap_or(Value::Null),
        )
        .unwrap_or(0.0);
        atr_data(atr, close, symbol)
    }

    async fn fetch_hyperliquid(&self, info_client: &InfoClient, symbol: &str) -> Result<AtrData> {
        let candles = info_client
            .fetch_hl_candles(symbol, &self.atr_interval)
            .await?;
        if candles.len() < self.atr_period + 1 {
            return Err(anyhow!("insufficient Hyperliquid candles for {}", symbol));
        }

        let mut trs = Vec::new();
        for i in 1..candles.len() {
            let high = candles[i].get("h").and_then(value_to_f64).unwrap_or(0.0);
            let low = candles[i].get("l").and_then(value_to_f64).unwrap_or(0.0);
            let prev_close = candles[i - 1]
                .get("c")
                .and_then(value_to_f64)
                .unwrap_or(0.0);
            trs.push(
                (high - low)
                    .max((high - prev_close).abs())
                    .max((low - prev_close).abs()),
            );
        }
        let atr = average_tail(&trs, self.atr_period)?;
        let close = candles
            .last()
            .and_then(|c| c.get("c"))
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        atr_data(atr, close, symbol)
    }
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
