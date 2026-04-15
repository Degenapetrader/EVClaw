use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use log::warn;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use tokio::process::Command;

use crate::config::Config;
use crate::hyperliquid::normalize_hl_symbol;
use crate::types::{AccountSummary, Direction, LivePosition, OrderResult};

#[derive(Debug, Clone)]
pub struct AcpAccount {
    pub buyer_address: String,
    pub hl_address: String,
    pub hl_balance: f64,
    pub withdrawable_balance: f64,
}

pub struct AcpExecutor {
    http: Client,
    api_key: String,
    trader_base_url: String,
    public_info_url: String,
    job_script_path: PathBuf,
    buyer_wallet: String,
    hl_address: Option<String>,
    last_good_summary: Mutex<Option<AccountSummary>>,
    dry_run: bool,
    read_retry_attempts: usize,
    read_retry_base_ms: u64,
}

impl AcpExecutor {
    pub fn new(cfg: &Config) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .build()
            .context("failed to build ACP reqwest client")?;
        Ok(Self {
            http,
            api_key: cfg
                .dgclaw_api_key
                .clone()
                .ok_or_else(|| anyhow!("missing DGCLAW_API_KEY"))?,
            trader_base_url: cfg.dgclaw_trader_base_url.trim_end_matches('/').to_string(),
            public_info_url: cfg.public_info_url.clone(),
            job_script_path: cfg.dgclaw_job_script_path.clone(),
            buyer_wallet: std::env::var("ACP_CLIENT_WALLET_ADDRESS")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| cfg.hl_address.clone())
                .ok_or_else(|| anyhow!("missing ACP buyer wallet address"))?,
            hl_address: None,
            last_good_summary: Mutex::new(None),
            dry_run: cfg.dry_run,
            read_retry_attempts: 5,
            read_retry_base_ms: cfg.rate_limit_backoff_ms.clamp(250, 1_000),
        })
    }

    pub fn hl_address(&self) -> Option<&str> {
        self.hl_address.as_deref()
    }

    pub async fn fetch_account_summary(&self) -> Result<Option<AccountSummary>> {
        let url = format!(
            "{}/users/{}/account",
            self.trader_base_url, self.buyer_wallet
        );
        let payload: Envelope<Value> = self.get_json_with_retry(&url).await?;
        let Some(data) = payload.data else {
            return Ok(None);
        };
        let wallet = data
            .get("buyerAddress")
            .and_then(Value::as_str)
            .unwrap_or(&self.buyer_wallet)
            .to_string();
        let account_value = data
            .get("hlBalance")
            .and_then(value_as_f64)
            .or_else(|| data.get("balance").and_then(value_as_f64))
            .unwrap_or(0.0);
        let withdrawable = data
            .get("withdrawableBalance")
            .and_then(value_as_f64)
            .or_else(|| data.get("withdrawable").and_then(value_as_f64))
            .unwrap_or(account_value);
        let margin_used = (account_value - withdrawable).max(0.0);
        let summary = AccountSummary {
            wallet,
            account_value,
            margin_used,
            available_margin: withdrawable.max(0.0),
            is_locked: false,
        };

        let mut cached = self
            .last_good_summary
            .lock()
            .map_err(|_| anyhow!("ACP account summary cache poisoned"))?;
        if summary.account_value > 0.0 || summary.available_margin > 0.0 {
            *cached = Some(summary.clone());
            return Ok(Some(summary));
        }
        if let Some(previous) = cached.clone() {
            warn!(
                "[ACP] /account returned zero summary, reusing cached account_value={:.4} available_margin={:.4}",
                previous.account_value, previous.available_margin
            );
            return Ok(Some(previous));
        }
        Ok(Some(summary))
    }

    pub async fn fetch_positions(&self) -> Result<HashMap<String, LivePosition>> {
        let url = format!(
            "{}/users/{}/positions",
            self.trader_base_url, self.buyer_wallet
        );
        let payload: Envelope<Value> = self.get_json_with_retry(&url).await?;
        let mut positions = HashMap::new();
        let Some(rows) = payload.data.and_then(|value| value.as_array().cloned()) else {
            return Ok(positions);
        };
        for row in rows {
            if let Some(position) = parse_live_position_row(&row) {
                positions.insert(position.symbol.clone(), position);
            }
        }
        Ok(positions)
    }

    pub async fn sync_account(&mut self) -> Result<AcpAccount> {
        let url = format!(
            "{}/users/{}/account",
            self.trader_base_url, self.buyer_wallet
        );
        let payload: Envelope<AcpAccountResponse> = self.get_json_with_retry(&url).await?;
        let data = payload
            .data
            .ok_or_else(|| anyhow!("missing account data from DegenClaw"))?;
        let account = AcpAccount {
            buyer_address: data.buyer_address,
            hl_address: data.hl_address,
            hl_balance: parse_opt_num(data.hl_balance.as_deref()),
            withdrawable_balance: parse_opt_num(data.withdrawable_balance.as_deref()),
        };
        if account.hl_balance > 0.0 || account.withdrawable_balance > 0.0 {
            let mut cached = self
                .last_good_summary
                .lock()
                .map_err(|_| anyhow!("ACP account summary cache poisoned"))?;
            *cached = Some(AccountSummary {
                wallet: account.buyer_address.clone(),
                account_value: account.hl_balance,
                margin_used: (account.hl_balance - account.withdrawable_balance).max(0.0),
                available_margin: account.withdrawable_balance.max(0.0),
                is_locked: false,
            });
        }
        self.hl_address = Some(account.hl_address.clone());
        Ok(account)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_limit_order(
        &self,
        pair: &str,
        side: Direction,
        size_usd: f64,
        limit_price: f64,
        leverage: Option<f64>,
        stop_loss: Option<f64>,
        take_profit: Option<f64>,
    ) -> Result<OrderResult> {
        let mut args = vec![
            "perp_trade".to_string(),
            "--submit-only".to_string(),
            "--action".to_string(),
            "open".to_string(),
            "--pair".to_string(),
            pair.to_string(),
            "--side".to_string(),
            match side {
                Direction::Long => "long",
                Direction::Short => "short",
                Direction::Neutral => return Err(anyhow!("cannot open neutral ACP order")),
            }
            .to_string(),
            "--size".to_string(),
            fmt_num(size_usd),
            "--orderType".to_string(),
            "limit".to_string(),
            "--limitPrice".to_string(),
            fmt_num(limit_price),
        ];
        if let Some(leverage) = leverage {
            args.push("--leverage".to_string());
            args.push(fmt_num(leverage));
        }
        if let Some(stop_loss) = stop_loss {
            args.push("--stopLoss".to_string());
            args.push(fmt_num(stop_loss));
        }
        if let Some(take_profit) = take_profit {
            args.push("--takeProfit".to_string());
            args.push(fmt_num(take_profit));
        }
        let payload = self.run_job(args).await?;
        Ok(OrderResult {
            success: true,
            order_id: payload
                .get("jobId")
                .and_then(Value::as_i64)
                .map(|id| id.to_string()),
            filled_size: 0.0,
            filled_price: 0.0,
            error: None,
        })
    }

    pub async fn cancel_limit_order(&self, pair: &str, order_id: &str) -> Result<OrderResult> {
        let payload = self
            .run_job(vec![
                "perp_trade".to_string(),
                "--submit-only".to_string(),
                "--action".to_string(),
                "cancel_limit".to_string(),
                "--pair".to_string(),
                pair.to_string(),
                "--oid".to_string(),
                order_id.to_string(),
            ])
            .await?;
        Ok(OrderResult {
            success: true,
            order_id: payload
                .get("jobId")
                .and_then(Value::as_i64)
                .map(|id| id.to_string()),
            filled_size: 0.0,
            filled_price: 0.0,
            error: None,
        })
    }

    pub async fn close_position(&self, pair: &str) -> Result<OrderResult> {
        let payload = self
            .run_job(vec![
                "perp_trade".to_string(),
                "--action".to_string(),
                "close".to_string(),
                "--pair".to_string(),
                pair.to_string(),
            ])
            .await?;
        Ok(OrderResult {
            success: payload.get("status").and_then(Value::as_str) == Some("completed"),
            order_id: payload
                .get("jobId")
                .and_then(Value::as_i64)
                .map(|id| id.to_string()),
            filled_size: 0.0,
            filled_price: 0.0,
            error: payload
                .get("rejectionReason")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        })
    }

    pub async fn modify_position(
        &self,
        pair: &str,
        leverage: Option<f64>,
        stop_loss: Option<f64>,
        take_profit: Option<f64>,
    ) -> Result<OrderResult> {
        let mut args = vec![
            "perp_modify".to_string(),
            "--submit-only".to_string(),
            "--pair".to_string(),
            pair.to_string(),
        ];
        if let Some(leverage) = leverage {
            args.push("--leverage".to_string());
            args.push(fmt_num(leverage));
        }
        if let Some(stop_loss) = stop_loss {
            args.push("--stopLoss".to_string());
            args.push(fmt_num(stop_loss));
        }
        if let Some(take_profit) = take_profit {
            args.push("--takeProfit".to_string());
            args.push(fmt_num(take_profit));
        }
        let child_id = self.spawn_job(args).await?;
        Ok(OrderResult {
            success: true,
            order_id: child_id.map(|id| id.to_string()),
            filled_size: 0.0,
            filled_price: 0.0,
            error: None,
        })
    }

    pub async fn active_entry_symbols(&self) -> Result<HashSet<String>> {
        let jobs = match self.fetch_active_jobs().await {
            Ok(jobs) => jobs,
            Err(err) => {
                warn!("[ACP] active_jobs lookup failed: {err:#}");
                return Ok(HashSet::new());
            }
        };
        let mut symbols = HashSet::new();
        for job in jobs {
            if job
                .rejection_reason
                .as_deref()
                .is_some_and(|reason| !reason.trim().is_empty())
            {
                continue;
            }
            let name = job.name.as_deref().unwrap_or_default();
            if name != "perp_trade" {
                continue;
            }
            let requirement = job.requirement.as_ref().and_then(Value::as_object);
            let action = requirement
                .and_then(|req| req.get("action"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            if action != "open" {
                continue;
            }
            let pair = requirement
                .and_then(|req| req.get("pair"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            if pair.is_empty() {
                continue;
            }
            symbols.insert(normalize_hl_symbol(pair));
        }
        Ok(symbols)
    }

    pub async fn fetch_active_jobs(&self) -> Result<Vec<AcpJobStatus>> {
        let payload = match self.run_job(vec!["active_jobs".to_string()]).await {
            Ok(payload) => payload,
            Err(err) => {
                return Err(err);
            }
        };
        let Some(jobs) = payload.get("jobs").and_then(Value::as_array) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for job in jobs {
            match serde_json::from_value::<AcpJobStatus>(job.clone()) {
                Ok(job) => out.push(job),
                Err(err) => warn!("[ACP] failed to decode active job entry: {err}"),
            }
        }
        Ok(out)
    }

    pub async fn get_job_status(&self, job_id: &str) -> Result<AcpJobStatus> {
        let payload = self
            .run_job(vec![
                "job_status".to_string(),
                "--jobId".to_string(),
                job_id.to_string(),
            ])
            .await?;
        serde_json::from_value(payload).context("failed to decode ACP job status")
    }

    async fn run_job(&self, mut args: Vec<String>) -> Result<Value> {
        if self.dry_run && !args.iter().any(|arg| arg == "--dry-run") {
            args.push("--dry-run".to_string());
        }
        let max_attempts = 4usize;
        let mut delay_ms = self.read_retry_base_ms.max(500);
        for attempt in 1..=max_attempts {
            let output = Command::new("node")
                .arg(&self.job_script_path)
                .args(&args)
                .stdin(Stdio::null())
                .output()
                .await
                .with_context(|| format!("failed to run {}", self.job_script_path.display()))?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.success() {
                return extract_last_json_object(&stdout).with_context(|| {
                    format!("failed to parse ACP job output stderr={}", stderr.trim())
                });
            }

            let err_text = format!("stderr={} stdout={}", stderr.trim(), stdout.trim());
            let retryable = is_retryable_acp_job_error(&err_text);
            if retryable && attempt < max_attempts {
                warn!(
                    "[ACP] helper command retry {}/{} in {}ms args={:?} err={}",
                    attempt, max_attempts, delay_ms, args, err_text
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms.saturating_mul(2)).min(10_000);
                continue;
            }

            return Err(anyhow!(
                "ACP job command failed status={} stderr={} stdout={}",
                output.status,
                stderr.trim(),
                stdout.trim()
            ));
        }
        Err(anyhow!("ACP helper command exhausted retries"))
    }

    async fn spawn_job(&self, mut args: Vec<String>) -> Result<Option<u32>> {
        if self.dry_run && !args.iter().any(|arg| arg == "--dry-run") {
            args.push("--dry-run".to_string());
        }
        let child = Command::new("node")
            .arg(&self.job_script_path)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to spawn {}", self.job_script_path.display()))?;
        Ok(child.id())
    }

    async fn get_json_with_retry<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let mut delay_ms = self.read_retry_base_ms;
        for attempt in 1..=self.read_retry_attempts {
            let response = self.http.get(url).bearer_auth(&self.api_key).send().await;
            match response {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        return response
                            .json::<T>()
                            .await
                            .with_context(|| format!("failed to decode {}", url));
                    }

                    let body = response.text().await.unwrap_or_default();
                    let retryable = status.as_u16() == 429 || status.is_server_error();
                    if !retryable || attempt == self.read_retry_attempts {
                        return Err(anyhow!(
                            "GET {} returned status={} body={}",
                            url,
                            status,
                            body
                        ));
                    }
                    warn!(
                        "[ACP] GET {} attempt {}/{} returned status={} retrying in {}ms",
                        url, attempt, self.read_retry_attempts, status, delay_ms
                    );
                }
                Err(err) => {
                    if attempt == self.read_retry_attempts {
                        return Err(err).with_context(|| format!("GET {} failed", url));
                    }
                    warn!(
                        "[ACP] GET {} attempt {}/{} failed: {} retrying in {}ms",
                        url, attempt, self.read_retry_attempts, err, delay_ms
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms.saturating_mul(2)).min(5_000);
        }
        Err(anyhow!("GET {} exhausted retries", url))
    }

    pub async fn fetch_top_of_book_price(&self, pair: &str, side: Direction) -> Result<f64> {
        let response = self
            .http
            .post(&self.public_info_url)
            .json(&serde_json::json!({
                "type": "l2Book",
                "coin": pair,
            }))
            .send()
            .await
            .with_context(|| format!("POST {} l2Book failed", self.public_info_url))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "POST {} l2Book failed status={} body={}",
                self.public_info_url,
                status,
                body
            ));
        }
        let payload: Value = response
            .json()
            .await
            .context("failed to decode l2Book response")?;
        let levels = payload
            .get("levels")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("l2Book missing levels for {}", pair))?;
        let (level_idx, label) = match side {
            Direction::Long => (0usize, "bid"),
            Direction::Short => (1usize, "ask"),
            Direction::Neutral => return Err(anyhow!("cannot price neutral ACP limit order")),
        };
        let level = levels
            .get(level_idx)
            .and_then(Value::as_array)
            .and_then(|entries| entries.first())
            .ok_or_else(|| anyhow!("l2Book missing top {} for {}", label, pair))?;
        level
            .get("px")
            .and_then(value_as_f64)
            .or_else(|| level.get(0).and_then(value_as_f64))
            .ok_or_else(|| anyhow!("l2Book top {} missing price for {}", label, pair))
    }
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcpAccountResponse {
    buyer_address: String,
    hl_address: String,
    hl_balance: Option<String>,
    withdrawable_balance: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AcpJobStatus {
    #[serde(rename = "jobId")]
    pub job_id: Option<String>,
    pub phase: String,
    pub name: Option<String>,
    #[serde(rename = "rejectionReason")]
    pub rejection_reason: Option<String>,
    pub requirement: Option<Value>,
}

fn is_retryable_acp_job_error(err_text: &str) -> bool {
    let lower = err_text.to_ascii_lowercase();
    lower.contains("over rate limit")
        || lower.contains("too many requests")
        || lower.contains("429")
        || lower.contains("network error")
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_opt_num(text: Option<&str>) -> f64 {
    text.and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn fmt_num(value: f64) -> String {
    let mut out = format!("{:.8}", value);
    while out.contains('.') && out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.pop();
    }
    out
}

fn extract_last_json_object(stdout: &str) -> Result<Value> {
    let trimmed = stdout.trim();
    for (idx, _) in trimmed.match_indices('{').rev() {
        if let Ok(value) = serde_json::from_str::<Value>(&trimmed[idx..]) {
            return Ok(value);
        }
    }
    Err(anyhow!("no JSON object found in output"))
}

fn parse_live_position_row(row: &Value) -> Option<LivePosition> {
    let pair = first_str(
        row,
        &["pair", "symbol", "coin", "asset", "market", "ticker"],
    )?;
    let exchange_symbol = pair.to_string();
    let symbol = normalize_hl_symbol(pair);

    let entry_price = first_f64(
        row,
        &[
            "entryPrice",
            "avgEntryPrice",
            "averageEntryPrice",
            "avgPx",
            "entryPx",
        ],
    )
    .unwrap_or(0.0);
    let mark_price = first_f64(row, &["markPrice", "markPx", "mark", "px"]).unwrap_or(0.0);
    let notional_value = first_f64(
        row,
        &[
            "notionalValue",
            "positionValue",
            "value",
            "notional",
            "notionalSize",
            "sizeUsd",
            "usdValue",
            "margin",
        ],
    )
    .unwrap_or(0.0);
    let qty = first_f64(
        row,
        &[
            "qty",
            "size",
            "sz",
            "positionSize",
            "positionQty",
            "quantity",
            "baseSize",
        ],
    )
    .or_else(|| {
        let px = if mark_price > 0.0 {
            mark_price
        } else {
            entry_price
        };
        if px > 0.0 && notional_value > 0.0 {
            Some(notional_value.abs() / px)
        } else {
            None
        }
    })
    .unwrap_or(0.0);
    let side = first_str(row, &["side", "direction", "positionSide", "dir"]);
    let direction = direction_from_side(side, qty);
    if !direction.is_actionable() || qty.abs() <= 0.0 {
        return None;
    }
    let unrealized_pnl = first_f64(
        row,
        &["unrealizedPnl", "unrealizedPnL", "upl", "pnl", "upnl"],
    )
    .unwrap_or(0.0);
    let notional_value = if notional_value > 0.0 {
        notional_value
    } else {
        qty.abs() * entry_price
    };

    Some(LivePosition {
        symbol,
        exchange_symbol,
        qty: qty.abs(),
        direction,
        entry_price,
        unrealized_pnl,
        notional_value,
    })
}

fn first_str<'a>(row: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(value) = row.get(*key).and_then(Value::as_str) {
            return Some(value);
        }
    }
    None
}

fn first_f64(row: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(value) = row.get(*key).and_then(value_as_f64) {
            return Some(value);
        }
    }
    None
}

fn direction_from_side(side: Option<&str>, qty: f64) -> Direction {
    match side.map(|value| value.trim().to_ascii_lowercase()) {
        Some(value) if value.contains("long") || value == "buy" || value == "b" => Direction::Long,
        Some(value) if value.contains("short") || value == "sell" || value == "s" => {
            Direction::Short
        }
        _ => Direction::from_signed_qty(qty),
    }
}
