use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use ethers::signers::LocalWallet;
use ethers::types::H160;
use hyperliquid_rust_sdk::{
    BaseUrl, ClientCancelRequest, ClientLimit, ClientOrder, ClientOrderRequest, ClientTrigger,
    ExchangeClient, ExchangeDataStatus, ExchangeResponseStatus,
};
use log::{debug, info, warn};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::{Mutex, Semaphore};

use crate::config::Config;
use crate::types::{
    AccountSummary, Direction, LivePosition, MarketMeta, OpenOrder, OrderResult, UserFill,
    WalletPosition,
};

#[derive(Debug, Clone)]
struct OrderStatus {
    status: String,
    filled: f64,
    remaining: f64,
    average: f64,
}

const ORDER_CANCEL_CONFIRM_RETRIES: usize = 3;
const ORDER_CANCEL_CONFIRM_DELAY_MS: u64 = 250;
const ORDER_FILL_SETTLE_RETRIES: usize = 4;
const ORDER_FILL_SETTLE_DELAY_MS: u64 = 250;
const ORDER_FILL_LOOKBACK_MS: u64 = 5_000;
const WALLET_SNAPSHOT_CACHE_MAX_AGE_SECS: u64 = 60;
const PRICE_CACHE_MAX_AGE_SECS: u64 = 60;
const BINANCE_TICKER_PRICE_URL: &str = "https://fapi.binance.com/fapi/v1/ticker/price";

#[derive(Debug, Clone, Default)]
struct WalletSnapshotData {
    positions_by_symbol: HashMap<String, Vec<WalletPosition>>,
    account_summaries: HashMap<String, AccountSummary>,
    wallet_signature: u64,
    fetched_at: Option<Instant>,
}

impl WalletSnapshotData {
    fn age(&self) -> Option<Duration> {
        self.fetched_at.map(|ts| ts.elapsed())
    }

    fn is_stale(&self) -> bool {
        self.age()
            .map(|age| age > Duration::from_secs(WALLET_SNAPSHOT_CACHE_MAX_AGE_SECS))
            .unwrap_or(true)
    }
}

#[derive(Debug, Default)]
struct WalletSnapshotCacheState {
    data: Option<WalletSnapshotData>,
    refreshing: bool,
}

#[derive(Debug, Default)]
struct PriceCache {
    prices: HashMap<String, f64>,
    source: &'static str,
    fetched_at: Option<Instant>,
}

impl PriceCache {
    fn is_stale(&self) -> bool {
        self.fetched_at
            .map(|ts| ts.elapsed() > Duration::from_secs(PRICE_CACHE_MAX_AGE_SECS))
            .unwrap_or(true)
    }

    fn set_prices(&mut self, prices: HashMap<String, f64>, source: &'static str) {
        self.prices = prices;
        self.source = source;
        self.fetched_at = Some(Instant::now());
    }
}

pub struct InfoClient {
    public_info_url: String,
    private_info_url: Option<String>,
    user: Option<String>,
    http: Client,
    max_wallet_concurrency: usize,
    wallet_batch_size: usize,
    wallet_snapshot_cache: Arc<Mutex<WalletSnapshotCacheState>>,
}

impl InfoClient {
    pub fn new(cfg: &Config) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self {
            public_info_url: cfg.public_info_url.clone(),
            private_info_url: cfg.private_info_url.clone(),
            user: cfg.hl_address.clone(),
            http,
            max_wallet_concurrency: cfg.max_wallet_concurrency,
            wallet_batch_size: cfg.wallet_batch_size,
            wallet_snapshot_cache: Arc::new(Mutex::new(WalletSnapshotCacheState::default())),
        })
    }

    pub async fn fetch_market_meta(
        &self,
        symbol_filter: Option<&HashSet<String>>,
    ) -> Result<HashMap<String, MarketMeta>> {
        let meta = self.post_public(json!({ "type": "meta" })).await?;
        let universe = meta
            .get("universe")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("meta missing universe"))?;
        let ctx_data = self
            .post_public(json!({ "type": "metaAndAssetCtxs" }))
            .await?;
        let ctxs = ctx_data
            .get(1)
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("metaAndAssetCtxs missing asset ctxs"))?;

        let mut out = HashMap::new();
        for (idx, asset) in universe.iter().enumerate() {
            let Some(exchange_symbol) = asset.get("name").and_then(Value::as_str) else {
                continue;
            };
            let symbol = normalize_hl_symbol(exchange_symbol);
            if let Some(filter) = symbol_filter {
                if !filter.contains(&symbol) {
                    continue;
                }
            }

            let ctx = ctxs.get(idx).unwrap_or(&Value::Null);
            let oi = ctx
                .get("openInterest")
                .and_then(value_as_f64)
                .unwrap_or(0.0);
            let mark_px = ctx.get("markPx").and_then(value_as_f64).unwrap_or(0.0);
            let oi_usd = oi * mark_px;
            let sz_decimals = asset.get("szDecimals").and_then(Value::as_u64).unwrap_or(4) as u32;
            let price_decimals = asset
                .get("pxDecimals")
                .and_then(Value::as_u64)
                .map(|value| value as u32)
                .unwrap_or(6u32.saturating_sub(sz_decimals.min(6)));
            let min_size = 10f64.powi(-(sz_decimals.min(12) as i32));
            let tick_size = 10f64.powi(-(price_decimals.min(12) as i32));

            out.insert(
                symbol,
                MarketMeta {
                    exchange_symbol: exchange_symbol.to_string(),
                    mark_px,
                    oi_usd,
                    sz_decimals,
                    price_decimals,
                    min_size,
                    tick_size,
                },
            );
        }
        Ok(out)
    }

    pub async fn fetch_user_fills(&self, since_ms: Option<u64>) -> Result<Vec<UserFill>> {
        let Some(user) = self.user.as_ref() else {
            return Ok(Vec::new());
        };
        let raw = if let Some(start_time) = since_ms {
            let by_time_payload = json!({
                "type": "userFillsByTime",
                "user": user,
                "startTime": start_time,
                "endTime": crate::now_ms(),
                "aggregateByTime": false,
            });
            match self.post_public(by_time_payload).await {
                Ok(value) => value,
                Err(by_time_err) => {
                    debug!("userFillsByTime failed, falling back to userFills: {by_time_err}");
                    self.post_public(json!({
                        "type": "userFills",
                        "user": user,
                        "startTime": start_time,
                    }))
                    .await?
                }
            }
        } else {
            self.post_public(json!({
                "type": "userFills",
                "user": user,
            }))
            .await?
        };
        Ok(parse_user_fills_array(
            raw.as_array().map(|arr| arr.as_slice()).unwrap_or(&[]),
        ))
    }

    pub async fn fetch_wallet_snapshot(
        &self,
        wallets: &HashSet<String>,
    ) -> Result<(
        HashMap<String, Vec<WalletPosition>>,
        HashMap<String, AccountSummary>,
    )> {
        let wallet_signature = wallet_set_signature(wallets);
        let cached = {
            let mut cache = self.wallet_snapshot_cache.lock().await;
            let snapshot = cache.data.clone();
            let needs_refresh = snapshot
                .as_ref()
                .map(|data| data.wallet_signature != wallet_signature || data.is_stale())
                .unwrap_or(true);
            if needs_refresh && !cache.refreshing {
                cache.refreshing = true;
                self.spawn_wallet_snapshot_refresh(wallets.clone(), wallet_signature);
            }
            snapshot
        };

        if let Some(snapshot) = cached {
            if snapshot.wallet_signature == wallet_signature {
                return Ok((snapshot.positions_by_symbol, snapshot.account_summaries));
            }
        }

        Ok((HashMap::new(), HashMap::new()))
    }

    pub async fn fetch_live_account_positions(&self) -> Result<HashMap<String, LivePosition>> {
        let Some(user) = self.user.as_ref() else {
            return Ok(HashMap::new());
        };
        let payload = json!({ "type": "clearinghouseState", "user": user });
        let data = post_private_first(
            &self.http,
            self.private_info_url.as_deref(),
            &self.public_info_url,
            &payload,
        )
        .await?;
        parse_live_positions(&data)
    }

    pub async fn fetch_live_account_summary(&self) -> Result<Option<AccountSummary>> {
        let Some(user) = self.user.as_ref() else {
            return Ok(None);
        };
        let payload = json!({ "type": "clearinghouseState", "user": user });
        let data = post_private_first(
            &self.http,
            self.private_info_url.as_deref(),
            &self.public_info_url,
            &payload,
        )
        .await?;
        Ok(Some(parse_account_summary(user.clone(), &data)))
    }

    pub async fn fetch_hl_candles(&self, symbol: &str, interval: &str) -> Result<Vec<Value>> {
        let payload = json!({
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": 0,
                "endTime": crate::now_ms()
            }
        });
        let data = self.post_public(payload).await?;
        data.as_array()
            .cloned()
            .ok_or_else(|| anyhow!("candleSnapshot response was not an array"))
    }

    async fn post_public(&self, payload: Value) -> Result<Value> {
        post_json(&self.http, &self.public_info_url, &payload).await
    }

    fn spawn_wallet_snapshot_refresh(&self, wallets: HashSet<String>, wallet_signature: u64) {
        let target_url = self
            .private_info_url
            .clone()
            .unwrap_or_else(|| self.public_info_url.clone());
        let cache = Arc::clone(&self.wallet_snapshot_cache);
        let client = self.http.clone();
        let max_wallet_concurrency = self.max_wallet_concurrency;
        let wallet_batch_size = self.wallet_batch_size;
        std::thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build wallet snapshot runtime")
                .and_then(|runtime| {
                    runtime.block_on(fetch_wallet_snapshot_data(
                        client,
                        target_url,
                        wallets,
                        wallet_signature,
                        max_wallet_concurrency,
                        wallet_batch_size,
                    ))
                });
            let mut state = cache.blocking_lock();
            state.refreshing = false;
            match result {
                Ok(snapshot) => state.data = Some(snapshot),
                Err(err) => debug!("wallet snapshot refresh failed: {err}"),
            }
        });
    }
}

pub struct ExecutionClient {
    exchange: Option<ExchangeClient>,
    dry_run: bool,
    public_info_url: String,
    private_info_url: Option<String>,
    trading_address: Option<String>,
    http: Client,
    chase_limit_timeout_secs: u64,
    chase_check_interval_ms: u64,
    chase_replace_ticks: f64,
    rate_limit_backoff_ms: u64,
    backoff_until: Option<Instant>,
    symbol_sz_decimals: HashMap<String, u32>,
    symbol_price_decimals: HashMap<String, u32>,
    symbol_min_sizes: HashMap<String, f64>,
    exchange_symbols: HashMap<String, String>,
    unsupported_symbols: HashSet<String>,
    price_cache: PriceCache,
}

impl ExecutionClient {
    pub async fn new(cfg: &Config) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .build()
            .context("failed to build reqwest client")?;

        let exchange = if cfg.dry_run {
            None
        } else {
            let private_key = cfg
                .hl_agent_private_key
                .as_deref()
                .ok_or_else(|| anyhow!("missing EVCLAW_AGENT_PRIVATE_KEY"))?;
            let signer = LocalWallet::from_str(private_key)
                .map_err(|e| anyhow!("failed to parse EVCLAW_AGENT_PRIVATE_KEY: {e}"))?;
            let vault = match cfg.hl_vault_address.as_deref() {
                Some(addr) if !addr.trim().is_empty() => Some(
                    H160::from_str(addr)
                        .map_err(|e| anyhow!("failed to parse EVCLAW_VAULT_ADDRESS: {e}"))?,
                ),
                _ => None,
            };
            Some(
                ExchangeClient::new(None, signer, Some(BaseUrl::Mainnet), None, vault)
                    .await
                    .context("failed to initialize Hyperliquid ExchangeClient")?,
            )
        };

        Ok(Self {
            exchange,
            dry_run: cfg.dry_run,
            public_info_url: cfg.public_info_url.clone(),
            private_info_url: cfg.private_info_url.clone(),
            trading_address: cfg.hl_address.clone(),
            http,
            chase_limit_timeout_secs: cfg.chase_limit_timeout_secs,
            chase_check_interval_ms: cfg.chase_check_interval_ms,
            chase_replace_ticks: cfg.chase_replace_ticks,
            rate_limit_backoff_ms: cfg.rate_limit_backoff_ms,
            backoff_until: None,
            symbol_sz_decimals: HashMap::new(),
            symbol_price_decimals: HashMap::new(),
            symbol_min_sizes: HashMap::new(),
            exchange_symbols: HashMap::new(),
            unsupported_symbols: HashSet::new(),
            price_cache: PriceCache::default(),
        })
    }

    pub fn register_market_meta(&mut self, meta: &HashMap<String, MarketMeta>) {
        for (symbol, market) in meta {
            self.symbol_sz_decimals
                .insert(symbol.clone(), market.sz_decimals);
            self.symbol_price_decimals
                .insert(symbol.clone(), market.price_decimals);
            self.symbol_min_sizes
                .insert(symbol.clone(), market.min_size);
            self.exchange_symbols
                .insert(symbol.clone(), market.exchange_symbol.clone());
        }
    }

    pub fn is_supported_symbol(&self, symbol: &str) -> bool {
        !self.unsupported_symbols.contains(symbol)
    }

    pub fn round_size(&self, size: f64, symbol: &str) -> f64 {
        let decimals = self
            .symbol_sz_decimals
            .get(symbol)
            .copied()
            .unwrap_or(4)
            .min(12);
        round_decimals(size, decimals)
    }

    pub fn round_price(&self, price: f64, symbol: &str) -> f64 {
        normalized_price(price, self.max_price_decimals(symbol)).unwrap_or(price)
    }

    pub fn min_size(&self, symbol: &str) -> f64 {
        self.symbol_min_sizes.get(symbol).copied().unwrap_or(0.0001)
    }

    pub async fn get_all_positions(&self) -> Result<HashMap<String, LivePosition>> {
        let Some(user) = self.trading_address.as_ref() else {
            return Ok(HashMap::new());
        };
        let payload = json!({ "type": "clearinghouseState", "user": user });
        let data = post_private_first(
            &self.http,
            self.private_info_url.as_deref(),
            &self.public_info_url,
            &payload,
        )
        .await?;
        parse_live_positions(&data)
    }

    pub async fn get_position(&self, symbol: &str) -> Result<Option<LivePosition>> {
        let positions = self.get_all_positions().await?;
        Ok(positions.get(symbol).cloned())
    }

    pub async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<OpenOrder>> {
        let Some(user) = self.trading_address.as_ref() else {
            return Ok(Vec::new());
        };
        let payload = json!({ "type": "openOrders", "user": user });
        let data = post_private_first(
            &self.http,
            self.private_info_url.as_deref(),
            &self.public_info_url,
            &payload,
        )
        .await?;
        let Some(items) = data.as_array() else {
            return Ok(Vec::new());
        };

        let target_exchange_symbol = symbol.and_then(|sym| self.exchange_symbol(sym));
        let mut out = Vec::new();
        for item in items {
            let Some(exchange_symbol) = item.get("coin").and_then(Value::as_str) else {
                continue;
            };
            if let Some(target_exchange_symbol) = target_exchange_symbol {
                if target_exchange_symbol != exchange_symbol {
                    continue;
                }
            }
            let canonical = normalize_hl_symbol(exchange_symbol);
            let reduce_only = item
                .get("reduceOnly")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut order_type = if reduce_only {
                "unknown_reduce_only".to_string()
            } else {
                "limit".to_string()
            };
            if let Some(trigger) = item
                .get("orderType")
                .and_then(|value| value.get("trigger"))
                .and_then(Value::as_object)
            {
                match trigger.get("tpsl").and_then(Value::as_str) {
                    Some("sl") => order_type = "sl".to_string(),
                    Some("tp") => order_type = "tp".to_string(),
                    _ => {}
                }
            } else if let Some(tpsl) = item.get("tpsl").and_then(Value::as_str) {
                order_type = tpsl.to_string();
            } else if item
                .get("isTrigger")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                || item.get("triggerPx").is_some()
            {
                order_type = "sl".to_string();
            }

            let price = item
                .get("triggerPx")
                .and_then(value_as_f64)
                .or_else(|| item.get("limitPx").and_then(value_as_f64))
                .unwrap_or(0.0);
            let size = item.get("sz").and_then(value_as_f64).unwrap_or(0.0);
            let order_id = item
                .get("oid")
                .and_then(Value::as_u64)
                .map(|oid| oid.to_string())
                .unwrap_or_default();

            out.push(OpenOrder {
                order_id,
                order_type,
                reduce_only,
                price,
                size,
                symbol: canonical,
            });
        }
        Ok(out)
    }

    pub async fn cancel_all_limit_orders(&mut self, symbol: Option<&str>) -> Result<usize> {
        let orders = self.get_open_orders(symbol).await?;
        let mut cancelled = 0usize;
        for order in orders {
            if order.order_type != "limit" || order.reduce_only {
                continue;
            }
            if self
                .cancel_order_with_confirmation(&order.symbol, &order.order_id)
                .await?
            {
                cancelled = cancelled.saturating_add(1);
            }
        }
        Ok(cancelled)
    }

    pub async fn cancel_order(&mut self, symbol: &str, order_id: &str) -> Result<bool> {
        if self.dry_run {
            info!("[DRY] cancel order symbol={} oid={order_id}", symbol);
            return Ok(true);
        }
        self.wait_rate_limit_backoff().await;

        let exchange_symbol = match self.exchange_symbol(symbol) {
            Some(symbol) => symbol.to_string(),
            None => return Ok(false),
        };
        let oid: u64 = match order_id.parse() {
            Ok(oid) if oid > 0 => oid,
            _ => return Ok(false),
        };
        let exchange = self
            .exchange
            .as_mut()
            .ok_or_else(|| anyhow!("exchange client unavailable for cancel_order"))?;
        let response = match exchange
            .bulk_cancel(
                vec![ClientCancelRequest {
                    asset: exchange_symbol,
                    oid,
                }],
                None,
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let err_text = err.to_string();
                if is_rate_limited_error(&err_text) {
                    self.trigger_rate_limit_backoff();
                    return Err(anyhow!("cancel rate limited for {symbol}: {err_text}"));
                }
                if is_terminal_cancel_error(&err_text) {
                    return Ok(true);
                }
                return Err(anyhow!("bulk_cancel failed for {symbol}: {err_text}"));
            }
        };

        match response {
            ExchangeResponseStatus::Ok(resp) => {
                if let Some(data) = resp.data {
                    for status in data.statuses {
                        match status {
                            ExchangeDataStatus::Success => return Ok(true),
                            ExchangeDataStatus::Error(err) => {
                                if is_rate_limited_error(&err) {
                                    self.trigger_rate_limit_backoff();
                                    return Err(anyhow!("cancel rate limited for {symbol}: {err}"));
                                }
                                if is_terminal_cancel_error(&err) {
                                    return Ok(true);
                                }
                                return Err(anyhow!("cancel rejected for {symbol}: {err}"));
                            }
                            other => {
                                debug!("cancel status for {}: {:?}", symbol, other);
                            }
                        }
                    }
                }
                Ok(true)
            }
            ExchangeResponseStatus::Err(err) => {
                if is_rate_limited_error(&err) {
                    self.trigger_rate_limit_backoff();
                    return Err(anyhow!("cancel rate limited for {symbol}: {err}"));
                }
                if is_terminal_cancel_error(&err) {
                    return Ok(true);
                }
                Err(anyhow!("cancel exchange error for {symbol}: {err}"))
            }
        }
    }

    pub async fn place_stop_order(
        &mut self,
        symbol: &str,
        side: Direction,
        size: f64,
        trigger_price: f64,
        order_type: &str,
    ) -> Result<OrderResult> {
        if size <= 0.0 || trigger_price <= 0.0 {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                filled_size: 0.0,
                filled_price: 0.0,
                error: Some("invalid size/price".to_string()),
            });
        }
        if self.dry_run {
            info!(
                "[DRY] {} {} size={:.6} symbol={} trigger={:.6}",
                order_type, side, size, symbol, trigger_price
            );
            return Ok(OrderResult {
                success: true,
                order_id: Some(format!("dry-{}-{}", order_type, crate::now_ms())),
                filled_size: 0.0,
                filled_price: trigger_price,
                error: None,
            });
        }
        self.wait_rate_limit_backoff().await;

        let exchange_symbol = self
            .exchange_symbol(symbol)
            .ok_or_else(|| anyhow!("missing exchange symbol for {symbol}"))?
            .to_string();
        let is_buy = side == Direction::Long;
        let size = self.round_size(size, symbol);
        let trigger_price = self.round_price(trigger_price, symbol);
        let max_price_decimals = self.max_price_decimals(symbol);
        let exchange = self
            .exchange
            .as_mut()
            .ok_or_else(|| anyhow!("exchange client unavailable for place_stop_order"))?;
        let normalized_trigger_px = normalized_limit_price(trigger_price, max_price_decimals)
            .ok_or_else(|| anyhow!("failed to normalize trigger price for {}", symbol))?;
        let req = ClientOrderRequest {
            asset: exchange_symbol,
            is_buy,
            reduce_only: true,
            limit_px: normalized_trigger_px,
            sz: size,
            cloid: None,
            order_type: ClientOrder::Trigger(ClientTrigger {
                is_market: order_type == "sl",
                trigger_px: normalized_trigger_px,
                tpsl: if order_type == "sl" {
                    "sl".to_string()
                } else {
                    "tp".to_string()
                },
            }),
        };

        let response = match exchange.bulk_order(vec![req], None).await {
            Ok(response) => response,
            Err(err) => {
                let err_text = err.to_string();
                if is_rate_limited_error(&err_text) {
                    self.trigger_rate_limit_backoff();
                }
                return Err(anyhow!("bulk_order stop failed for {symbol}: {err_text}"));
            }
        };

        self.parse_order_response(symbol, response, normalized_trigger_px, size)
    }

    pub async fn place_market_order(
        &mut self,
        symbol: &str,
        side: Direction,
        size: f64,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        if !side.is_actionable() || size <= 0.0 {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                filled_size: 0.0,
                filled_price: 0.0,
                error: Some("invalid side/size".to_string()),
            });
        }
        if self.unsupported_symbols.contains(symbol) {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                filled_size: 0.0,
                filled_price: 0.0,
                error: Some("unsupported symbol".to_string()),
            });
        }
        if self.dry_run {
            let px = self.fetch_mid_price(symbol).await?.unwrap_or(0.0);
            info!(
                "[DRY] MARKET {} size={:.6} symbol={} reduce_only={} px={:.6}",
                side, size, symbol, reduce_only, px
            );
            return Ok(OrderResult {
                success: true,
                order_id: Some(format!("dry-market-{}", crate::now_ms())),
                filled_size: self.round_size(size, symbol),
                filled_price: px,
                error: None,
            });
        }

        self.wait_rate_limit_backoff().await;

        if let Err(err) = self
            .clear_resting_limit_orders(symbol, "pre-chase cleanup")
            .await
        {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                filled_size: 0.0,
                filled_price: 0.0,
                error: Some(err.to_string()),
            });
        }

        let requested_size = self.round_size(size, symbol);
        if requested_size <= 0.0 {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                filled_size: 0.0,
                filled_price: 0.0,
                error: Some("size rounded to zero".to_string()),
            });
        }

        let pre_position = self.get_position(symbol).await?;
        let pre_size = pre_position.as_ref().map(|pos| pos.qty).unwrap_or(0.0);
        let pre_direction = pre_position
            .as_ref()
            .map(|pos| pos.direction)
            .unwrap_or(Direction::Neutral);

        let mut filled_total = 0.0;
        let mut filled_notional = 0.0;
        let mut current_order_id: Option<u64> = None;
        let mut current_limit_px: Option<f64> = None;
        let mut current_order_filled = 0.0;
        let mut current_order_started_at_ms: Option<u64> = None;
        let mut remaining = requested_size;
        let start = Instant::now();

        while start.elapsed() < Duration::from_secs(self.chase_limit_timeout_secs) {
            let Some(mid) = self.fetch_mid_price(symbol).await? else {
                tokio::time::sleep(Duration::from_millis(self.chase_check_interval_ms)).await;
                continue;
            };
            let tick_size = self.tick_size(symbol);

            if current_order_id.is_none() {
                if let Err(err) = self
                    .clear_resting_limit_orders(symbol, "pre-submit cleanup")
                    .await
                {
                    return Ok(OrderResult {
                        success: false,
                        order_id: None,
                        filled_size: filled_total,
                        filled_price: if filled_total > 0.0 {
                            filled_notional / filled_total
                        } else {
                            mid
                        },
                        error: Some(err.to_string()),
                    });
                }
                self.refresh_fill_from_live_position(
                    symbol,
                    pre_size,
                    pre_direction,
                    reduce_only,
                    requested_size,
                    mid,
                    &mut filled_total,
                    &mut filled_notional,
                    &mut remaining,
                )
                .await?;
                if remaining <= 0.0 {
                    self.clear_resting_limit_orders(symbol, "position delta fill")
                        .await?;
                    let avg_px = if filled_total > 0.0 {
                        filled_notional / filled_total
                    } else {
                        mid
                    };
                    return Ok(OrderResult {
                        success: true,
                        order_id: None,
                        filled_size: filled_total,
                        filled_price: avg_px,
                        error: None,
                    });
                }

                let raw_limit_px = match side {
                    Direction::Long => (mid - tick_size).max(tick_size),
                    Direction::Short => mid + tick_size,
                    Direction::Neutral => mid,
                };
                let limit_px =
                    normalized_limit_price(raw_limit_px, self.max_price_decimals(symbol))
                        .ok_or_else(|| anyhow!("failed to normalize limit price for {}", symbol))?;
                let response = self
                    .submit_limit_order(symbol, side, remaining, limit_px, reduce_only)
                    .await?;
                if !response.success {
                    return Ok(response);
                }
                current_order_id = response
                    .order_id
                    .as_deref()
                    .and_then(|oid| oid.parse().ok());
                current_limit_px = Some(limit_px);
                current_order_filled = response.filled_size;
                current_order_started_at_ms = Some(crate::now_ms());
                if response.filled_size > 0.0 {
                    filled_total += response.filled_size;
                    filled_notional += response.filled_size * response.filled_price.max(limit_px);
                    remaining = (requested_size - filled_total).max(0.0);
                    if remaining <= 0.0 {
                        self.clear_resting_limit_orders(symbol, "submit fill")
                            .await?;
                        let avg_px = if filled_total > 0.0 {
                            filled_notional / filled_total
                        } else {
                            response.filled_price
                        };
                        return Ok(OrderResult {
                            success: true,
                            order_id: response.order_id,
                            filled_size: filled_total,
                            filled_price: avg_px,
                            error: None,
                        });
                    }
                }
            }

            if let Some(order_id) = current_order_id {
                if let Some(status) = self.get_order_status(order_id).await? {
                    let incremental = (status.filled - current_order_filled).max(0.0);
                    if incremental > 0.0 {
                        current_order_filled = status.filled;
                        filled_total += incremental;
                        filled_notional +=
                            incremental * status.average.max(current_limit_px.unwrap_or(mid));
                        remaining = (requested_size - filled_total).max(0.0);
                    }

                    if status.remaining <= 0.0
                        || matches!(status.status.as_str(), "filled" | "done" | "closed")
                    {
                        if let Some(started_at_ms) = current_order_started_at_ms {
                            self.reconcile_inactive_order_fill(
                                symbol,
                                order_id,
                                started_at_ms,
                                pre_size,
                                pre_direction,
                                reduce_only,
                                requested_size,
                                current_limit_px.unwrap_or(mid),
                                &mut filled_total,
                                &mut filled_notional,
                                &mut remaining,
                                &mut current_order_filled,
                            )
                            .await?;
                        }
                        current_order_id = None;
                        current_limit_px = None;
                        current_order_filled = 0.0;
                        current_order_started_at_ms = None;
                        if remaining <= 0.0 {
                            self.clear_resting_limit_orders(symbol, "status fill")
                                .await?;
                            let avg_px = if filled_total > 0.0 {
                                filled_notional / filled_total
                            } else {
                                status.average
                            };
                            return Ok(OrderResult {
                                success: true,
                                order_id: Some(order_id.to_string()),
                                filled_size: filled_total,
                                filled_price: avg_px,
                                error: None,
                            });
                        }
                    }
                }
            }

            if let (Some(order_id), Some(active_limit_px)) = (current_order_id, current_limit_px) {
                let ticks_moved = ((mid - active_limit_px).abs() / tick_size).abs();
                if ticks_moved >= self.chase_replace_ticks {
                    let cancelled = self
                        .cancel_order_with_confirmation(symbol, &order_id.to_string())
                        .await?;
                    if !cancelled {
                        return Ok(OrderResult {
                            success: false,
                            order_id: Some(order_id.to_string()),
                            filled_size: filled_total,
                            filled_price: if filled_total > 0.0 {
                                filled_notional / filled_total
                            } else {
                                active_limit_px
                            },
                            error: Some("failed to cancel resting chase order".to_string()),
                        });
                    }
                    if let Err(err) = self
                        .clear_resting_limit_orders(symbol, "replace cancel")
                        .await
                    {
                        return Ok(OrderResult {
                            success: false,
                            order_id: Some(order_id.to_string()),
                            filled_size: filled_total,
                            filled_price: if filled_total > 0.0 {
                                filled_notional / filled_total
                            } else {
                                active_limit_px
                            },
                            error: Some(err.to_string()),
                        });
                    }
                    if let Some(started_at_ms) = current_order_started_at_ms {
                        self.reconcile_inactive_order_fill(
                            symbol,
                            order_id,
                            started_at_ms,
                            pre_size,
                            pre_direction,
                            reduce_only,
                            requested_size,
                            active_limit_px,
                            &mut filled_total,
                            &mut filled_notional,
                            &mut remaining,
                            &mut current_order_filled,
                        )
                        .await?;
                    }
                    current_order_id = None;
                    current_limit_px = None;
                    current_order_filled = 0.0;
                    current_order_started_at_ms = None;
                    if remaining <= 0.0 {
                        let avg_px = if filled_total > 0.0 {
                            filled_notional / filled_total
                        } else {
                            mid
                        };
                        return Ok(OrderResult {
                            success: true,
                            order_id: Some(order_id.to_string()),
                            filled_size: filled_total,
                            filled_price: avg_px,
                            error: None,
                        });
                    }
                }
            }

            if remaining <= 0.0 {
                if let Some(order_id) = current_order_id {
                    let cancelled = self
                        .cancel_order_with_confirmation(symbol, &order_id.to_string())
                        .await?;
                    if !cancelled {
                        return Ok(OrderResult {
                            success: false,
                            order_id: Some(order_id.to_string()),
                            filled_size: filled_total,
                            filled_price: if filled_total > 0.0 {
                                filled_notional / filled_total
                            } else {
                                mid
                            },
                            error: Some(
                                "filled target but could not clear active chase order".to_string(),
                            ),
                        });
                    }
                    if let Err(err) = self
                        .clear_resting_limit_orders(symbol, "filled-target cleanup")
                        .await
                    {
                        return Ok(OrderResult {
                            success: false,
                            order_id: Some(order_id.to_string()),
                            filled_size: filled_total,
                            filled_price: if filled_total > 0.0 {
                                filled_notional / filled_total
                            } else {
                                mid
                            },
                            error: Some(err.to_string()),
                        });
                    }
                }
                let avg_px = if filled_total > 0.0 {
                    filled_notional / filled_total
                } else {
                    mid
                };
                return Ok(OrderResult {
                    success: true,
                    order_id: None,
                    filled_size: filled_total,
                    filled_price: avg_px,
                    error: None,
                });
            }

            tokio::time::sleep(Duration::from_millis(self.chase_check_interval_ms)).await;
        }

        if let Some(order_id) = current_order_id {
            let cancelled = self
                .cancel_order_with_confirmation(symbol, &order_id.to_string())
                .await?;
            if !cancelled {
                return Ok(OrderResult {
                    success: false,
                    order_id: Some(order_id.to_string()),
                    filled_size: filled_total,
                    filled_price: if filled_total > 0.0 {
                        filled_notional / filled_total
                    } else {
                        0.0
                    },
                    error: Some("chase-limit timeout with active resting order".to_string()),
                });
            }
            if let Some(started_at_ms) = current_order_started_at_ms {
                self.reconcile_inactive_order_fill(
                    symbol,
                    order_id,
                    started_at_ms,
                    pre_size,
                    pre_direction,
                    reduce_only,
                    requested_size,
                    current_limit_px.unwrap_or(0.0),
                    &mut filled_total,
                    &mut filled_notional,
                    &mut remaining,
                    &mut current_order_filled,
                )
                .await?;
            }
        }
        if self
            .clear_resting_limit_orders(symbol, "chase timeout")
            .await
            .is_err()
        {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                filled_size: filled_total,
                filled_price: if filled_total > 0.0 {
                    filled_notional / filled_total
                } else {
                    0.0
                },
                error: Some("chase-limit timeout left resting orders on book".to_string()),
            });
        }

        if filled_total > 0.0 {
            let avg_px = filled_notional / filled_total;
            return Ok(OrderResult {
                success: true,
                order_id: current_order_id.map(|oid| oid.to_string()),
                filled_size: filled_total,
                filled_price: avg_px,
                error: None,
            });
        }

        let post_position = self.get_position(symbol).await?;
        if let Some(post_position) = post_position {
            let post_size = post_position.qty;
            let post_direction = post_position.direction;
            let actual_size = match (pre_direction, post_direction) {
                (Direction::Neutral, dir) if dir.is_actionable() => post_size,
                (dir_before, dir_after) if dir_before == dir_after => {
                    if reduce_only && post_size < pre_size {
                        pre_size - post_size
                    } else {
                        (post_size - pre_size).max(0.0)
                    }
                }
                (_, dir_after) if dir_after.is_actionable() => pre_size + post_size,
                _ => pre_size,
            };
            if actual_size > 0.0 {
                return Ok(OrderResult {
                    success: true,
                    order_id: None,
                    filled_size: actual_size,
                    filled_price: post_position.entry_price.max(0.0),
                    error: None,
                });
            }
        }

        Ok(OrderResult {
            success: false,
            order_id: None,
            filled_size: 0.0,
            filled_price: 0.0,
            error: Some("chase-limit timeout".to_string()),
        })
    }

    pub fn is_rate_limited_error(err: &anyhow::Error) -> bool {
        err.chain()
            .any(|cause| is_rate_limited_error(&cause.to_string()))
    }

    fn tick_size(&self, symbol: &str) -> f64 {
        let decimals = self.max_price_decimals(symbol);
        10f64.powi(-(decimals.min(12) as i32))
    }

    async fn submit_limit_order(
        &mut self,
        symbol: &str,
        side: Direction,
        size: f64,
        limit_px: f64,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        self.wait_rate_limit_backoff().await;

        let exchange_symbol = self
            .exchange_symbol(symbol)
            .ok_or_else(|| anyhow!("missing exchange symbol for {symbol}"))?
            .to_string();
        let exchange = self
            .exchange
            .as_mut()
            .ok_or_else(|| anyhow!("exchange client unavailable for submit_limit_order"))?;
        let response = match exchange
            .bulk_order(
                vec![ClientOrderRequest {
                    asset: exchange_symbol,
                    is_buy: side == Direction::Long,
                    reduce_only,
                    limit_px,
                    sz: size,
                    cloid: None,
                    order_type: ClientOrder::Limit(ClientLimit {
                        tif: "Gtc".to_string(),
                    }),
                }],
                None,
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let err_text = err.to_string();
                if self.mark_unsupported_if_needed(symbol, &err_text) {
                    return Ok(OrderResult {
                        success: false,
                        order_id: None,
                        filled_size: 0.0,
                        filled_price: 0.0,
                        error: Some(err_text),
                    });
                }
                if is_rate_limited_error(&err_text) {
                    self.trigger_rate_limit_backoff();
                }
                return Err(anyhow!("bulk_order failed for {symbol}: {err_text}"));
            }
        };
        self.parse_order_response(symbol, response, limit_px, size)
    }

    fn parse_order_response(
        &mut self,
        symbol: &str,
        response: ExchangeResponseStatus,
        default_price: f64,
        default_size: f64,
    ) -> Result<OrderResult> {
        match response {
            ExchangeResponseStatus::Ok(resp) => {
                if let Some(data) = resp.data {
                    for status in data.statuses {
                        match status {
                            ExchangeDataStatus::Filled(order) => {
                                return Ok(OrderResult {
                                    success: true,
                                    order_id: Some(order.oid.to_string()),
                                    filled_size: order
                                        .total_sz
                                        .parse::<f64>()
                                        .unwrap_or(default_size),
                                    filled_price: order
                                        .avg_px
                                        .parse::<f64>()
                                        .unwrap_or(default_price),
                                    error: None,
                                });
                            }
                            ExchangeDataStatus::Resting(order) => {
                                return Ok(OrderResult {
                                    success: true,
                                    order_id: Some(order.oid.to_string()),
                                    filled_size: 0.0,
                                    filled_price: default_price,
                                    error: None,
                                });
                            }
                            ExchangeDataStatus::Error(err) => {
                                if self.mark_unsupported_if_needed(symbol, &err) {
                                    return Ok(OrderResult {
                                        success: false,
                                        order_id: None,
                                        filled_size: 0.0,
                                        filled_price: 0.0,
                                        error: Some(err),
                                    });
                                }
                                if is_rate_limited_error(&err) {
                                    self.trigger_rate_limit_backoff();
                                }
                                return Ok(OrderResult {
                                    success: false,
                                    order_id: None,
                                    filled_size: 0.0,
                                    filled_price: 0.0,
                                    error: Some(err),
                                });
                            }
                            other => debug!("order status for {}: {:?}", symbol, other),
                        }
                    }
                }
                Ok(OrderResult {
                    success: false,
                    order_id: None,
                    filled_size: 0.0,
                    filled_price: 0.0,
                    error: Some(format!("no executable order status for {symbol}")),
                })
            }
            ExchangeResponseStatus::Err(err) => {
                if self.mark_unsupported_if_needed(symbol, &err) {
                    return Ok(OrderResult {
                        success: false,
                        order_id: None,
                        filled_size: 0.0,
                        filled_price: 0.0,
                        error: Some(err),
                    });
                }
                if is_rate_limited_error(&err) {
                    self.trigger_rate_limit_backoff();
                }
                Ok(OrderResult {
                    success: false,
                    order_id: None,
                    filled_size: 0.0,
                    filled_price: 0.0,
                    error: Some(err),
                })
            }
        }
    }

    async fn fetch_mid_price(&mut self, symbol: &str) -> Result<Option<f64>> {
        if self.price_cache.prices.is_empty() || self.price_cache.is_stale() {
            if let Err(err) = self.refresh_price_cache().await {
                debug!("price cache refresh failed: {err}");
            }
        }

        if let Some(price) = self.price_cache.prices.get(symbol).copied() {
            return Ok(Some(price));
        }

        self.refresh_price_cache().await?;
        Ok(self.price_cache.prices.get(symbol).copied())
    }

    async fn cancel_order_with_confirmation(
        &mut self,
        symbol: &str,
        order_id: &str,
    ) -> Result<bool> {
        let mut cancelled = self.cancel_order(symbol, order_id).await?;
        for _ in 0..ORDER_CANCEL_CONFIRM_RETRIES {
            if !self.is_order_still_open(symbol, order_id).await? {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(ORDER_CANCEL_CONFIRM_DELAY_MS)).await;
            cancelled = self.cancel_order(symbol, order_id).await? || cancelled;
        }
        Ok(cancelled && !self.is_order_still_open(symbol, order_id).await?)
    }

    async fn is_order_still_open(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let open_orders = self.get_open_orders(Some(symbol)).await?;
        Ok(open_orders.iter().any(|order| {
            order.order_type == "limit" && !order.reduce_only && order.order_id == order_id
        }))
    }

    async fn has_resting_limit_orders(&self, symbol: &str) -> Result<bool> {
        let open_orders = self.get_open_orders(Some(symbol)).await?;
        Ok(open_orders
            .iter()
            .any(|order| order.order_type == "limit" && !order.reduce_only))
    }

    async fn clear_resting_limit_orders(&mut self, symbol: &str, context: &str) -> Result<()> {
        let mut total_cleared = 0usize;
        for attempt in 0..=ORDER_CANCEL_CONFIRM_RETRIES {
            total_cleared += self.cancel_all_limit_orders(Some(symbol)).await?;
            if !self.has_resting_limit_orders(symbol).await? {
                if total_cleared > 0 {
                    warn!(
                        "[{}] cancelled {} residual limit orders after {}",
                        symbol, total_cleared, context
                    );
                }
                return Ok(());
            }
            if attempt < ORDER_CANCEL_CONFIRM_RETRIES {
                tokio::time::sleep(Duration::from_millis(ORDER_CANCEL_CONFIRM_DELAY_MS)).await;
            }
        }
        Err(anyhow!(
            "{context} left resting limit orders on book for {symbol}"
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn refresh_fill_from_live_position(
        &self,
        symbol: &str,
        pre_size: f64,
        pre_direction: Direction,
        reduce_only: bool,
        requested_size: f64,
        reference_price: f64,
        filled_total: &mut f64,
        filled_notional: &mut f64,
        remaining: &mut f64,
    ) -> Result<()> {
        let Some((actual_size, actual_price)) = self
            .current_position_delta(symbol, pre_size, pre_direction, reduce_only)
            .await?
        else {
            return Ok(());
        };
        if actual_size <= *filled_total + order_qty_epsilon(actual_size) {
            return Ok(());
        }
        let incremental = (actual_size - *filled_total).max(0.0);
        *filled_total = actual_size;
        *filled_notional += incremental * actual_price.max(reference_price);
        *remaining = (requested_size - *filled_total).max(0.0);
        Ok(())
    }

    async fn current_position_delta(
        &self,
        symbol: &str,
        pre_size: f64,
        pre_direction: Direction,
        reduce_only: bool,
    ) -> Result<Option<(f64, f64)>> {
        let Some(post_position) = self.get_position(symbol).await? else {
            return Ok(None);
        };
        let post_size = post_position.qty;
        let post_direction = post_position.direction;
        let actual_size = match (pre_direction, post_direction) {
            (Direction::Neutral, dir) if dir.is_actionable() => post_size,
            (dir_before, dir_after) if dir_before == dir_after => {
                if reduce_only && post_size < pre_size {
                    pre_size - post_size
                } else {
                    (post_size - pre_size).max(0.0)
                }
            }
            (_, dir_after) if dir_after.is_actionable() => pre_size + post_size,
            _ => pre_size,
        };
        if actual_size > order_qty_epsilon(actual_size) {
            Ok(Some((actual_size, post_position.entry_price.max(0.0))))
        } else {
            Ok(None)
        }
    }

    async fn get_order_status(&self, oid: u64) -> Result<Option<OrderStatus>> {
        let Some(user) = self.trading_address.as_ref() else {
            return Ok(None);
        };
        let payload = json!({ "type": "orderStatus", "user": user, "oid": oid });
        let data = post_json(&self.http, &self.public_info_url, &payload).await?;
        let Some(order) = data.get("order") else {
            return Ok(None);
        };
        let size = order.get("sz").and_then(value_as_f64).unwrap_or(0.0);
        let remaining = order
            .get("remainingSz")
            .and_then(value_as_f64)
            .unwrap_or(0.0);
        Ok(Some(OrderStatus {
            status: order
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            filled: (size - remaining).max(0.0),
            remaining,
            average: order.get("avgPx").and_then(value_as_f64).unwrap_or(0.0),
        }))
    }

    async fn fetch_order_fills(&self, oid: u64, since_ms: u64) -> Result<Vec<UserFill>> {
        let Some(user) = self.trading_address.as_ref() else {
            return Ok(Vec::new());
        };
        let raw = match post_json(
            &self.http,
            &self.public_info_url,
            &json!({
                "type": "userFillsByTime",
                "user": user,
                "startTime": since_ms,
                "endTime": crate::now_ms(),
                "aggregateByTime": false,
            }),
        )
        .await
        {
            Ok(value) => value,
            Err(by_time_err) => {
                debug!("userFillsByTime failed for oid {}: {}", oid, by_time_err);
                post_json(
                    &self.http,
                    &self.public_info_url,
                    &json!({
                        "type": "userFills",
                        "user": user,
                        "startTime": since_ms,
                    }),
                )
                .await?
            }
        };
        Ok(
            parse_user_fills_array(raw.as_array().map(|arr| arr.as_slice()).unwrap_or(&[]))
                .into_iter()
                .filter(|fill| fill.oid == Some(oid))
                .collect(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn reconcile_inactive_order_fill(
        &self,
        symbol: &str,
        order_id: u64,
        order_started_at_ms: u64,
        pre_size: f64,
        pre_direction: Direction,
        reduce_only: bool,
        requested_size: f64,
        reference_price: f64,
        filled_total: &mut f64,
        filled_notional: &mut f64,
        remaining: &mut f64,
        current_order_filled: &mut f64,
    ) -> Result<()> {
        for attempt in 0..=ORDER_FILL_SETTLE_RETRIES {
            if let Some(status) = self.get_order_status(order_id).await? {
                let status_filled = status.filled.max(0.0);
                if status_filled > *current_order_filled + order_qty_epsilon(status_filled) {
                    let incremental = (status_filled - *current_order_filled).max(0.0);
                    *current_order_filled = status_filled;
                    *filled_total += incremental;
                    *filled_notional += incremental * status.average.max(reference_price);
                }
            }

            let fills = self
                .fetch_order_fills(
                    order_id,
                    order_started_at_ms.saturating_sub(ORDER_FILL_LOOKBACK_MS),
                )
                .await
                .unwrap_or_default();
            if !fills.is_empty() {
                let fills_total = fills.iter().map(|fill| fill.size).sum::<f64>();
                if fills_total > *current_order_filled + order_qty_epsilon(fills_total) {
                    let avg_fill_price =
                        fills.iter().map(|fill| fill.price * fill.size).sum::<f64>() / fills_total;
                    let incremental = (fills_total - *current_order_filled).max(0.0);
                    *current_order_filled = fills_total;
                    *filled_total += incremental;
                    *filled_notional += incremental * avg_fill_price.max(reference_price);
                }
            }

            self.refresh_fill_from_live_position(
                symbol,
                pre_size,
                pre_direction,
                reduce_only,
                requested_size,
                reference_price,
                filled_total,
                filled_notional,
                remaining,
            )
            .await?;
            *remaining = (requested_size - *filled_total).max(0.0);
            if *remaining <= order_qty_epsilon(requested_size) {
                *remaining = 0.0;
                return Ok(());
            }
            if attempt < ORDER_FILL_SETTLE_RETRIES {
                tokio::time::sleep(Duration::from_millis(ORDER_FILL_SETTLE_DELAY_MS)).await;
            }
        }
        Ok(())
    }

    async fn wait_rate_limit_backoff(&mut self) {
        if let Some(until) = self.backoff_until {
            let now = Instant::now();
            if until > now {
                tokio::time::sleep(until - now).await;
            }
            self.backoff_until = None;
        }
    }

    fn trigger_rate_limit_backoff(&mut self) {
        if self.rate_limit_backoff_ms == 0 {
            return;
        }
        self.backoff_until =
            Some(Instant::now() + Duration::from_millis(self.rate_limit_backoff_ms));
    }

    fn exchange_symbol(&self, symbol: &str) -> Option<&str> {
        self.exchange_symbols.get(symbol).map(String::as_str)
    }

    fn max_price_decimals(&self, symbol: &str) -> u32 {
        self.symbol_price_decimals
            .get(symbol)
            .copied()
            .unwrap_or_else(|| {
                let sz_decimals = self.symbol_sz_decimals.get(symbol).copied().unwrap_or(0);
                6u32.saturating_sub(sz_decimals.min(6))
            })
            .min(12)
    }

    fn mark_unsupported_if_needed(&mut self, symbol: &str, err_text: &str) -> bool {
        if !is_asset_not_found_text(err_text) {
            return false;
        }
        if self.unsupported_symbols.insert(symbol.to_string()) {
            let exchange_symbol = self.exchange_symbol(symbol).unwrap_or(symbol);
            info!(
                "[EXEC] marking symbol unsupported canonical={} exchange={} err={}",
                symbol, exchange_symbol, err_text
            );
        }
        true
    }

    async fn refresh_price_cache(&mut self) -> Result<()> {
        match post_json(
            &self.http,
            &self.public_info_url,
            &json!({ "type": "allMids" }),
        )
        .await
        {
            Ok(data) => {
                if let Some(prices) = parse_hl_mid_prices(&data) {
                    self.price_cache.set_prices(prices, "HL");
                    return Ok(());
                }
            }
            Err(err) => debug!("allMids refresh failed: {err}"),
        }

        let response = self
            .http
            .get(BINANCE_TICKER_PRICE_URL)
            .send()
            .await
            .context("GET Binance ticker price failed")?;
        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!(
                "GET {} failed status={}",
                BINANCE_TICKER_PRICE_URL,
                status
            ));
        }
        let rows: Vec<Value> = response
            .json()
            .await
            .context("failed to decode Binance ticker response")?;
        let mut prices = HashMap::new();
        for row in rows {
            let Some(symbol) = row.get("symbol").and_then(Value::as_str) else {
                continue;
            };
            let Some(price) = row.get("price").and_then(value_as_f64) else {
                continue;
            };
            let Some(canonical) = binance_symbol_to_hl(symbol) else {
                continue;
            };
            prices.insert(canonical, price);
        }
        if prices.is_empty() {
            return Err(anyhow!("Binance ticker price response was empty"));
        }
        self.price_cache.set_prices(prices, "BINANCE");
        Ok(())
    }
}

async fn fetch_wallet_snapshot_data(
    client: Client,
    url: String,
    wallets: HashSet<String>,
    wallet_signature: u64,
    max_wallet_concurrency: usize,
    wallet_batch_size: usize,
) -> Result<WalletSnapshotData> {
    let semaphore = Arc::new(Semaphore::new(max_wallet_concurrency));
    let wallet_vec: Vec<String> = wallets.into_iter().collect();
    let mut positions_by_symbol: HashMap<String, Vec<WalletPosition>> = HashMap::new();
    let mut account_summaries: HashMap<String, AccountSummary> = HashMap::new();

    for batch in wallet_vec.chunks(wallet_batch_size) {
        let mut tasks = Vec::with_capacity(batch.len());
        for wallet in batch {
            let client = client.clone();
            let url = url.clone();
            let semaphore = Arc::clone(&semaphore);
            let wallet = wallet.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.ok();
                fetch_wallet_state(client, url, wallet).await
            }));
        }

        for task in tasks {
            match task.await {
                Ok(Ok((positions, summary))) => {
                    for pos in positions {
                        positions_by_symbol
                            .entry(pos.symbol.clone())
                            .or_default()
                            .push(pos);
                    }
                    if let Some(summary) = summary {
                        account_summaries.insert(summary.wallet.clone(), summary);
                    }
                }
                Ok(Err(err)) => debug!("wallet snapshot fetch failed: {err}"),
                Err(err) => debug!("wallet snapshot task failed: {err}"),
            }
        }
    }

    Ok(WalletSnapshotData {
        positions_by_symbol,
        account_summaries,
        wallet_signature,
        fetched_at: Some(Instant::now()),
    })
}

async fn fetch_wallet_state(
    client: Client,
    url: String,
    wallet: String,
) -> Result<(Vec<WalletPosition>, Option<AccountSummary>)> {
    let payload = json!({ "type": "clearinghouseState", "user": wallet });
    let data = post_json(&client, &url, &payload).await?;
    let summary = Some(parse_account_summary(wallet.clone(), &data));

    let mut positions = Vec::new();
    for asset_pos in data
        .get("assetPositions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(position) = asset_pos.get("position") else {
            continue;
        };
        let qty = position.get("szi").and_then(value_as_f64).unwrap_or(0.0);
        if qty == 0.0 {
            continue;
        }
        let symbol = position
            .get("coin")
            .and_then(Value::as_str)
            .map(normalize_hl_symbol)
            .unwrap_or_default();
        let funding_since_open = position
            .get("cumFunding")
            .and_then(|value| value.get("sinceOpen"))
            .and_then(value_as_f64)
            .unwrap_or(999_999.0)
            .abs();

        positions.push(WalletPosition {
            wallet: wallet.clone(),
            symbol,
            side: if qty > 0.0 {
                Direction::Long
            } else {
                Direction::Short
            },
            position_value: position
                .get("positionValue")
                .and_then(value_as_f64)
                .unwrap_or(0.0)
                .abs(),
            funding_since_open,
        });
    }

    Ok((positions, summary))
}

fn parse_account_summary(wallet: String, data: &Value) -> AccountSummary {
    let cross_margin = data
        .get("crossMarginSummary")
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()));
    let account_value = cross_margin
        .get("accountValue")
        .and_then(value_as_f64)
        .unwrap_or(0.0);
    let margin_used = cross_margin
        .get("totalMarginUsed")
        .and_then(value_as_f64)
        .unwrap_or(0.0);
    let available_margin = account_value - margin_used;

    AccountSummary {
        wallet,
        account_value,
        margin_used,
        available_margin,
        is_locked: available_margin <= 0.0,
    }
}

fn parse_hl_mid_prices(data: &Value) -> Option<HashMap<String, f64>> {
    let map = data.as_object()?;
    let mut prices = HashMap::with_capacity(map.len());
    for (symbol, price) in map {
        let Some(price) = value_as_f64(price) else {
            continue;
        };
        prices.insert(normalize_hl_symbol(symbol), price);
    }
    Some(prices)
}

fn parse_live_positions(data: &Value) -> Result<HashMap<String, LivePosition>> {
    let mut out = HashMap::new();
    for asset_pos in data
        .get("assetPositions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(position) = asset_pos.get("position") else {
            continue;
        };
        let Some(exchange_symbol) = position.get("coin").and_then(Value::as_str) else {
            continue;
        };
        let signed_qty = position.get("szi").and_then(value_as_f64).unwrap_or(0.0);
        if signed_qty == 0.0 {
            continue;
        }
        let symbol = normalize_hl_symbol(exchange_symbol);
        let direction = Direction::from_signed_qty(signed_qty);
        out.insert(
            symbol.clone(),
            LivePosition {
                symbol,
                exchange_symbol: exchange_symbol.to_string(),
                qty: signed_qty.abs(),
                direction,
                entry_price: position
                    .get("entryPx")
                    .and_then(value_as_f64)
                    .unwrap_or(0.0),
                unrealized_pnl: position
                    .get("unrealizedPnl")
                    .and_then(value_as_f64)
                    .unwrap_or(0.0),
                notional_value: position
                    .get("positionValue")
                    .and_then(value_as_f64)
                    .unwrap_or(0.0)
                    .abs(),
            },
        );
    }
    Ok(out)
}

fn parse_user_fills_array(items: &[Value]) -> Vec<UserFill> {
    let mut fills = Vec::with_capacity(items.len());
    for item in items {
        let Some(exchange_symbol) = item.get("coin").and_then(Value::as_str) else {
            continue;
        };
        let price = item.get("px").and_then(value_as_f64).unwrap_or(0.0);
        let size = item.get("sz").and_then(value_as_f64).unwrap_or(0.0);
        let ts_ms = item.get("time").and_then(Value::as_u64).unwrap_or(0);
        if price <= 0.0 || size <= 0.0 || ts_ms == 0 {
            continue;
        }
        let tid = item.get("tid").and_then(Value::as_u64);
        let oid = item.get("oid").and_then(Value::as_u64);
        let fill_id = tid
            .map(|value| format!("tid:{value}"))
            .or_else(|| oid.map(|value| format!("oid:{value}:{ts_ms}")))
            .unwrap_or_else(|| format!("{}:{ts_ms}:{price:.8}:{size:.8}", exchange_symbol));
        fills.push(UserFill {
            fill_id,
            symbol: normalize_hl_symbol(exchange_symbol),
            exchange_symbol: exchange_symbol.to_string(),
            side: item
                .get("side")
                .and_then(Value::as_str)
                .map(direction_from_fill_side)
                .or_else(|| {
                    item.get("dir")
                        .and_then(Value::as_str)
                        .map(direction_from_fill_dir)
                })
                .unwrap_or(Direction::Neutral),
            price,
            size,
            ts_ms,
            oid,
            tid,
            fee_usd: item.get("fee").and_then(value_as_f64),
            builder_fee_usd: item.get("builderFee").and_then(value_as_f64),
            crossed: item
                .get("crossed")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            twap: item.get("twapId").is_some(),
        });
    }
    fills.sort_by_key(|fill| fill.ts_ms);
    fills
}

async fn post_json(client: &Client, url: &str, payload: &Value) -> Result<Value> {
    let response = client
        .post(url)
        .json(payload)
        .send()
        .await
        .with_context(|| format!("POST {} failed", url))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "POST {} failed status={} body={}",
            url,
            status,
            text
        ));
    }
    serde_json::from_str(&text).with_context(|| format!("failed to decode response from {}", url))
}

async fn post_private_first(
    client: &Client,
    private_url: Option<&str>,
    public_url: &str,
    payload: &Value,
) -> Result<Value> {
    if let Some(private_url) = private_url {
        match post_json(client, private_url, payload).await {
            Ok(data) => return Ok(data),
            Err(err) => debug!("private node request failed, falling back to public: {err}"),
        }
    }
    post_json(client, public_url, payload).await
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn normalized_limit_price(raw_px: f64, max_decimals: u32) -> Option<f64> {
    if !raw_px.is_finite() || raw_px <= 0.0 {
        return None;
    }
    normalized_price(raw_px, max_decimals)
}

fn normalized_price(raw_px: f64, max_decimals: u32) -> Option<f64> {
    if !raw_px.is_finite() || raw_px <= 0.0 {
        return None;
    }
    let px = round_significant(raw_px, 5);
    let px = round_decimals(px, max_decimals);
    (px > 0.0).then_some(px)
}

fn round_decimals(value: f64, decimals: u32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let factor = 10f64.powi(decimals.min(12) as i32);
    if !factor.is_finite() || factor <= 0.0 {
        return value;
    }
    (value * factor).round() / factor
}

fn order_qty_epsilon(qty: f64) -> f64 {
    qty.abs().max(1.0) * 1e-6
}

fn round_significant(value: f64, sig: u32) -> f64 {
    if !value.is_finite() || value <= 0.0 || sig == 0 {
        return value;
    }
    let abs_value = value.abs();
    let magnitude = abs_value.log10().floor() as i32;
    let scale = 10f64.powi(sig as i32 - magnitude - 1);
    if !scale.is_finite() || scale <= 0.0 {
        return value;
    }
    let rounded = (abs_value * scale).round() / scale;
    rounded.copysign(value)
}

fn wallet_set_signature(wallets: &HashSet<String>) -> u64 {
    let mut xor_acc = wallets.len() as u64;
    let mut sum_acc = 0u64;
    for wallet in wallets {
        let mut hasher = DefaultHasher::new();
        wallet.hash(&mut hasher);
        let hash = hasher.finish();
        xor_acc ^= hash.rotate_left((wallet.len() % 64) as u32);
        sum_acc = sum_acc.wrapping_add(hash);
    }
    xor_acc ^ sum_acc
}

fn binance_symbol_to_hl(symbol: &str) -> Option<String> {
    let base = symbol.strip_suffix("USDT")?;
    if let Some(rest) = base.strip_prefix("1000") {
        return Some(format!("k{}", rest.to_ascii_uppercase()));
    }
    Some(base.to_ascii_uppercase())
}

fn normalize_hl_symbol(symbol: &str) -> String {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Some(rest) = trimmed.strip_prefix('k') {
        return format!("k{}", rest.to_ascii_uppercase());
    }
    if let Some(rest) = trimmed.strip_prefix('K') {
        let candidate = format!("k{}", rest.to_ascii_uppercase());
        if is_known_k_symbol(&candidate) {
            return candidate;
        }
    }
    trimmed.to_ascii_uppercase()
}

fn direction_from_fill_side(side: &str) -> Direction {
    match side.trim().to_ascii_lowercase().as_str() {
        "b" | "buy" | "long" => Direction::Long,
        "s" | "sell" | "short" => Direction::Short,
        _ => Direction::Neutral,
    }
}

fn direction_from_fill_dir(dir: &str) -> Direction {
    let value = dir.trim().to_ascii_lowercase();
    if value.contains("buy") || value.contains("long") {
        Direction::Long
    } else if value.contains("sell") || value.contains("short") {
        Direction::Short
    } else {
        Direction::Neutral
    }
}

fn is_known_k_symbol(symbol: &str) -> bool {
    matches!(
        symbol,
        "kPEPE" | "kBONK" | "kSHIB" | "kFLOKI" | "kLUNC" | "kTOSHI" | "kXEC" | "kSATS"
    )
}

fn is_asset_not_found_text(text: &str) -> bool {
    text.to_ascii_lowercase().contains("asset not found")
}

fn is_rate_limited_error(text: &str) -> bool {
    let s = text.to_ascii_lowercase();
    s.contains("status code: 429")
        || s.contains("status=429")
        || s.contains("too many requests")
        || s.contains("too many cumulative requests")
        || s.contains("cumulative requests sent")
        || s.contains("free up 1 request per usdc traded")
        || (s.contains("rate") && s.contains("limit"))
}

fn is_terminal_cancel_error(text: &str) -> bool {
    let s = text.to_ascii_lowercase();
    s.contains("already canceled")
        || s.contains("already cancelled")
        || s.contains("canceled or filled")
        || s.contains("cancelled or filled")
        || s.contains("order was never placed")
        || s.contains("not found")
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{binance_symbol_to_hl, normalized_limit_price, wallet_set_signature};

    #[test]
    fn link_price_normalization_matches_hyperliquid_precision_rules() {
        let max_decimals = 5;
        assert_eq!(normalized_limit_price(8.59961, max_decimals), Some(8.5996));
        assert_eq!(normalized_limit_price(8.59929, max_decimals), Some(8.5993));
        assert_eq!(normalized_limit_price(66161.49, 2), Some(66161.0));
    }

    #[test]
    fn wallet_signature_is_order_independent() {
        let a: HashSet<String> = ["0x1".to_string(), "0x2".to_string()].into_iter().collect();
        let b: HashSet<String> = ["0x2".to_string(), "0x1".to_string()].into_iter().collect();
        assert_eq!(wallet_set_signature(&a), wallet_set_signature(&b));
    }

    #[test]
    fn binance_symbol_mapping_matches_tester_conventions() {
        assert_eq!(binance_symbol_to_hl("BTCUSDT"), Some("BTC".to_string()));
        assert_eq!(
            binance_symbol_to_hl("1000PEPEUSDT"),
            Some("kPEPE".to_string())
        );
        assert_eq!(
            binance_symbol_to_hl("1000BONKUSDT"),
            Some("kBONK".to_string())
        );
        assert_eq!(binance_symbol_to_hl("BTCUSD"), None);
    }
}
