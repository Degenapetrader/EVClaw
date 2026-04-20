use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use log::{info, warn};
use tokio::signal::unix::{signal, SignalKind};

use crate::acp_executor::{AcpExecutor, AcpJobStatus};
use crate::atr::AtrClient;
use crate::config::Config;
use crate::hyperliquid::{ExecutionClient, InfoClient};
use crate::journal::{TradeJournal, TradeRecord};
use crate::labels::WalletLabelStore;
use crate::signals::dead_cap::DeadCapitalSignal;
use crate::signals::whale::WhaleSignal;
use crate::state::{RuntimeState, StateStore};
use crate::types::{
    AccountDataSnapshot, AccountSummary, AtrCheckpointData, CombinedSignal, DeadCapSnapshot,
    Direction, LivePosition, MarketMeta, OiBucketPolicy, OpenOrder, SignalComponent,
    SymbolCooldown, TrackedPosition, UserFill,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryAttemptOutcome {
    Entered,
    Pending,
    Rejected,
    MarginExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitDisposition {
    Closed,
    Detached,
    KeepTracking,
}

const PRESSURE_DECAY_CHECK_INTERVAL_MS: u64 = 15 * 60 * 1_000;
const PRESSURE_DECAY_THRESHOLD_RATIO: f64 = 0.8;
const LATE_ENTRY_GUARD_8H_ATR_SURCHARGE: f64 = 2.0;
const LATE_ENTRY_GUARD_12H_ATR_SURCHARGE: f64 = 3.0;

const DISAPPEARED_FILL_LOOKBACK_MS: u64 = 15 * 60 * 1_000;
const DISAPPEARED_FILL_CLUSTER_MS: u64 = 2 * 60 * 1_000;

#[derive(Debug, Clone, Copy)]
struct LateEntryGuardMetrics {
    extension_4h_atr: f64,
    extension_8h_atr: f64,
    extension_12h_atr: f64,
    threshold_4h_atr: f64,
    threshold_8h_atr: f64,
    threshold_12h_atr: f64,
}

pub struct EvClawRuntime {
    cfg: Config,
    labels: WalletLabelStore,
    info_client: InfoClient,
    executor: ExecutionClient,
    acp_executor: Option<AcpExecutor>,
    atr_client: AtrClient,
    journal: TradeJournal,
    dead_cap: DeadCapitalSignal,
    whale: WhaleSignal,
    state_store: StateStore,
    state: RuntimeState,
    cycle_count: u64,
}

impl EvClawRuntime {
    pub async fn new(cfg: Config) -> Result<Self> {
        let mut labels = WalletLabelStore::new(cfg.scored_wallets_path.clone());
        labels.reload()?;

        let state_store = StateStore::new(state_path(&cfg.state_dir));
        let mut state = state_store.load()?;
        state.reentry_blocks.clear();
        let journal = TradeJournal::new(cfg.journal_path.clone())?;

        info!(
            "[startup] mode={} trading_address={} vault_address={}",
            if cfg.acp_mode { "acp" } else { "direct_hl" },
            cfg.hl_address.as_deref().unwrap_or("unset"),
            cfg.hl_vault_address.as_deref().unwrap_or("none"),
        );

        Ok(Self {
            info_client: InfoClient::new(&cfg)?,
            executor: ExecutionClient::new(&cfg).await?,
            acp_executor: if cfg.acp_mode {
                Some(AcpExecutor::new(&cfg)?)
            } else {
                None
            },
            atr_client: AtrClient::new(&cfg)?,
            journal,
            dead_cap: DeadCapitalSignal::default(),
            whale: WhaleSignal,
            state_store,
            cfg,
            labels,
            state,
            cycle_count: 0,
        })
    }

    pub async fn run(&mut self, once: bool) -> Result<()> {
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        loop {
            if let Err(err) = self.run_cycle().await {
                if once {
                    return Err(err);
                }
                warn!("cycle error: {err:#}");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
            if once {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(self.cfg.loop_interval_secs)) => {}
                _ = sigint.recv() => {
                    warn!("received SIGINT, stopping after current cycle");
                    break;
                }
                _ = sigterm.recv() => {
                    warn!("received SIGTERM, stopping after current cycle");
                    break;
                }
            }
        }
        self.log_summary("shutdown")?;
        Ok(())
    }

    async fn run_cycle(&mut self) -> Result<()> {
        self.cycle_count = self.cycle_count.saturating_add(1);
        let _ = self.labels.reload_if_changed()?;
        let base_tracked_wallets = self.labels.tracked_wallets();
        let tracked_wallets = self.labels.dead_cap_wallets(
            self.cfg.dead_cap_inactive_min_volume_usd,
            self.cfg.dead_cap_max_extra_wallets,
        );
        if self.cycle_count == 1 || tracked_wallets.len() != base_tracked_wallets.len() {
            info!(
                "[startup] dead_cap wallet coverage base={} expanded={} extra={}",
                base_tracked_wallets.len(),
                tracked_wallets.len(),
                tracked_wallets
                    .len()
                    .saturating_sub(base_tracked_wallets.len())
            );
        }

        let market_meta = self
            .info_client
            .fetch_market_meta(self.cfg.symbols.as_ref())
            .await?;
        self.executor.register_market_meta(&market_meta);
        if let Some(acp) = self.acp_executor.as_mut() {
            let account = acp.sync_account().await?;
            self.info_client.set_user(Some(account.hl_address.clone()));
            self.executor
                .set_trading_address(Some(account.hl_address.clone()));
        }
        if self.cycle_count == 1 && self.acp_executor.is_none() {
            let cancelled_limits = self.executor.cancel_all_limit_orders(None).await?;
            if cancelled_limits > 0 {
                warn!(
                    "[RECOVERY] cancelled {} stale limit orders on startup",
                    cancelled_limits
                );
            }
        }

        let (positions_by_symbol, account_summaries) = self
            .info_client
            .fetch_wallet_snapshot(&tracked_wallets)
            .await?;
        let snapshot = AccountDataSnapshot {
            market_meta,
            positions_by_symbol,
            account_summaries,
        };

        let signals = self.evaluate_signals(&snapshot);
        let (live_summary, live_positions) = if let Some(acp) = self.acp_executor.as_ref() {
            (
                acp.fetch_account_summary().await?,
                acp.fetch_positions().await?,
            )
        } else {
            (
                self.info_client.fetch_live_account_summary().await?,
                self.info_client.fetch_live_account_positions().await?,
            )
        };
        let active_acp_jobs = if let Some(acp) = self.acp_executor.as_ref() {
            match acp.fetch_active_jobs().await {
                Ok(jobs) => jobs,
                Err(err) => {
                    warn!("[ACP] active_jobs lookup failed: {err:#}");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        let active_acp_symbols = active_acp_open_symbols(&active_acp_jobs, &self.executor);

        self.release_reentry_blocks(&signals);
        self.release_symbol_cooldowns();
        let positions_changed = self
            .sync_tracked_positions(&live_positions, &snapshot.market_meta, &signals)
            .await?;
        self.refresh_acp_pending_entries(&live_positions, &active_acp_jobs)
            .await?;
        self.reconcile_acp_pending_entries(&signals, &live_positions, &active_acp_symbols)
            .await?;

        self.process_signal_exits(&signals).await?;
        self.process_entries(
            &snapshot,
            &signals,
            live_summary.as_ref(),
            &active_acp_symbols,
        )
        .await?;

        if positions_changed
            || self.cycle_count == 1
            || self
                .cycle_count
                .is_multiple_of(self.cfg.sltp_reconcile_every_cycles)
        {
            self.reconcile_sltp_orders(&active_acp_jobs).await?;
        }
        self.reconcile_fill_journal().await?;
        if let Err(err) = self
            .journal
            .log_dead_cap_history(self.state.dead_cap_snapshots.values().cloned())
        {
            warn!("dead-cap history log failed: {err:#}");
        }

        self.state_store.save(&self.state)?;
        if self.cycle_count.is_multiple_of(10) {
            self.log_summary("cycle")?;
        }
        info!(
            "cycle complete symbols={} open_positions={} reentry_blocks={} symbol_cooldowns={}",
            snapshot.market_meta.len(),
            self.state.positions.len(),
            self.state.reentry_blocks.len(),
            self.state.symbol_cooldowns.len()
        );
        Ok(())
    }

    fn evaluate_signals(
        &mut self,
        snapshot: &AccountDataSnapshot,
    ) -> HashMap<String, CombinedSignal> {
        let mut decisions = HashMap::new();
        let mut dead_cap_snapshots = HashMap::new();
        for (symbol, market) in &snapshot.market_meta {
            let positions = snapshot
                .positions_by_symbol
                .get(symbol)
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            let whale_eval = self
                .whale
                .evaluate(symbol, positions, market.oi_usd, &self.labels);
            let dead_eval = self.dead_cap.evaluate(
                symbol,
                positions,
                &snapshot.account_summaries,
                market.oi_usd,
                &self.labels,
            );

            let dead_component = SignalComponent {
                direction: dead_eval.signal,
                strength: dead_eval.strength,
                reason: dead_eval.reason.clone(),
            };
            let whale_component = SignalComponent {
                direction: whale_eval.signal,
                strength: whale_eval.strength,
                reason: whale_eval.reason,
            };
            let bucket_policy = oi_bucket_policy(market.oi_usd);
            let (net_score, target_direction, raw_notional_usd, order_notional_usd) =
                combined_target(
                    &dead_component,
                    &whale_component,
                    self.cfg.base_notional_usd,
                    self.cfg.min_trade_notional_usd,
                    &bucket_policy,
                );

            decisions.insert(
                symbol.clone(),
                CombinedSignal {
                    symbol: symbol.clone(),
                    exchange_symbol: market.exchange_symbol.clone(),
                    dead_cap: dead_component,
                    whale: whale_component,
                    net_score,
                    target_direction,
                    raw_notional_usd,
                    order_notional_usd,
                    reason: format!(
                        "net(dead-only)={:+.3} dead={}({:.2}) whale={}({:.2}) bucket={}",
                        net_score,
                        dead_eval.signal,
                        dead_eval.strength,
                        whale_eval.signal,
                        whale_eval.strength,
                        bucket_policy.bucket_label
                    ),
                },
            );
            dead_cap_snapshots.insert(
                symbol.clone(),
                DeadCapSnapshot {
                    symbol: symbol.clone(),
                    signal: dead_eval.signal,
                    strength: dead_eval.strength,
                    threshold: dead_eval.threshold,
                    locked_long_pct: dead_eval.locked_long_pct,
                    locked_short_pct: dead_eval.locked_short_pct,
                    effective_long_pct: dead_eval.effective_long_pct,
                    effective_short_pct: dead_eval.effective_short_pct,
                    bad_long_pct: dead_eval.bad_long_pct,
                    bad_short_pct: dead_eval.bad_short_pct,
                    smart_long_pct: dead_eval.smart_long_pct,
                    smart_short_pct: dead_eval.smart_short_pct,
                    observed_pct: dead_eval.observed_pct,
                    locked_wallet_count: dead_eval.locked_wallet_count,
                    dominant_top_share: dead_eval.dominant_top_share,
                    persistence_streak: dead_eval.persistence_streak,
                    reason: dead_eval.reason,
                },
            );
        }
        self.state.dead_cap_snapshots = dead_cap_snapshots;
        decisions
    }

    fn release_reentry_blocks(&mut self, signals: &HashMap<String, CombinedSignal>) {
        let mut released = Vec::new();
        for (symbol, blocked_direction) in &self.state.reentry_blocks {
            let target_direction = signals
                .get(symbol)
                .map(|signal| signal.target_direction)
                .unwrap_or(Direction::Neutral);
            if should_release_reentry_block(*blocked_direction, target_direction) {
                released.push(symbol.clone());
            }
        }
        for symbol in released {
            self.state.reentry_blocks.remove(&symbol);
        }
    }

    fn release_symbol_cooldowns(&mut self) {
        let now_ms = crate::now_ms();
        self.state
            .symbol_cooldowns
            .retain(|_, cooldown| cooldown.blocked_until_ms > now_ms);
    }

    fn is_symbol_cooldown_active(&self, symbol: &str, now_ms: u64) -> bool {
        self.state
            .symbol_cooldowns
            .get(symbol)
            .map(|cooldown| cooldown.blocked_until_ms > now_ms)
            .unwrap_or(false)
    }

    fn start_symbol_cooldown(&mut self, symbol: &str, reason: &str) {
        let blocked_until_ms =
            crate::now_ms().saturating_add(self.cfg.reentry_cooldown_secs.saturating_mul(1_000));
        self.state.symbol_cooldowns.insert(
            symbol.to_string(),
            SymbolCooldown {
                symbol: symbol.to_string(),
                blocked_until_ms,
                reason: reason.to_string(),
            },
        );
        self.state.reentry_blocks.remove(symbol);
    }

    async fn sync_tracked_positions(
        &mut self,
        live_positions: &HashMap<String, LivePosition>,
        market_meta: &HashMap<String, MarketMeta>,
        signals: &HashMap<String, CombinedSignal>,
    ) -> Result<bool> {
        let tracked_symbols: Vec<String> = self.state.positions.keys().cloned().collect();
        let mut remove_symbols = Vec::new();
        let mut changed = false;
        let now_ms = crate::now_ms();
        let recent_user_fills = if self.acp_executor.is_none() {
            self.executor
                .fetch_user_fills(Some(now_ms.saturating_sub(DISAPPEARED_FILL_LOOKBACK_MS)))
                .await
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        for symbol in tracked_symbols {
            let Some(tracked) = self.state.positions.get(&symbol).cloned() else {
                continue;
            };
            let fallback_policy = market_meta
                .get(&symbol)
                .map(|meta| oi_bucket_policy(meta.oi_usd))
                .unwrap_or_else(|| tracked.bucket_policy.clone());
            let tracked_policy = resolved_bucket_policy(&tracked.bucket_policy, &fallback_policy);
            match live_positions.get(&symbol) {
                None => {
                    let recent_fill_price =
                        recent_disappearance_fill_price(&tracked, &recent_user_fills, now_ms);
                    let (exit_price, exit_reason) = classify_disappeared_position(
                        &tracked,
                        market_meta.get(&symbol).map(|meta| meta.mark_px),
                        recent_fill_price,
                        tracked_policy.sl_atr_multiplier,
                        tracked_policy.tp_atr_multiplier,
                    );
                    info!(
                        "[{}] live position disappeared; classifying as {} @ {:.6}{}",
                        symbol,
                        exit_reason,
                        exit_price,
                        recent_fill_price
                            .map(|price| format!(" via recent fill {:.6}", price))
                            .unwrap_or_default()
                    );
                    let exit_signal = signals.get(&symbol);
                    let trade_id = match tracked.trade_id {
                        Some(trade_id) => Some(trade_id),
                        None => self.journal.find_open_trade(&symbol)?.map(|trade| trade.id),
                    };
                    if let Some(trade_id) = trade_id {
                        let _ = self.journal.close_trade(
                            trade_id,
                            exit_price,
                            &exit_reason,
                            exit_signal,
                        )?;
                    }
                    self.journal.log_event(
                        trade_id,
                        &symbol,
                        "POSITION_DISAPPEARED",
                        &format!("classified missing live position as {}", exit_reason),
                        exit_signal,
                    )?;
                    if should_start_symbol_cooldown(&exit_reason) {
                        self.start_symbol_cooldown(&symbol, &exit_reason);
                    }
                    remove_symbols.push(symbol);
                    changed = true;
                }
                Some(live) if live.direction != tracked.direction => {
                    warn!(
                        "[{}] live direction {} != tracked {}; detaching tracked state",
                        symbol, live.direction, tracked.direction
                    );
                    let trade_id = match tracked.trade_id {
                        Some(trade_id) => Some(trade_id),
                        None => self.journal.find_open_trade(&symbol)?.map(|trade| trade.id),
                    };
                    if let Some(trade_id) = trade_id {
                        let _ = self.journal.close_trade(
                            trade_id,
                            live.entry_price.max(tracked.entry_price),
                            "DIRECTION_CHANGED",
                            signals.get(&symbol),
                        )?;
                    }
                    remove_symbols.push(symbol);
                    changed = true;
                }
                Some(live) => {
                    let existing_open_trade = self.journal.find_open_trade(&symbol)?;
                    let existing_trade_id = self
                        .state
                        .positions
                        .get(&symbol)
                        .and_then(|position| position.trade_id)
                        .or(existing_open_trade.as_ref().map(|trade| trade.id));
                    if let Some(tracked_mut) = self.state.positions.get_mut(&symbol) {
                        if should_refresh_bucket_policy(&tracked_mut.bucket_policy)
                            || (tracked_mut.bucket_policy.min_hold_hours
                                - tracked_policy.min_hold_hours)
                                .abs()
                                > f64::EPSILON
                        {
                            tracked_mut.bucket_policy = tracked_policy.clone();
                            changed = true;
                        }
                        let mut trade_fill_sync_needed = false;
                        if let Some(open_trade) = existing_open_trade.as_ref() {
                            if (open_trade.size - live.qty).abs() > live_qty_epsilon(live.qty)
                                || (live.entry_price > 0.0
                                    && (open_trade.entry_price - live.entry_price).abs()
                                        > live.entry_price.abs().max(1.0) * 1e-8)
                            {
                                trade_fill_sync_needed = true;
                            }
                        }
                        if (live.qty - tracked_mut.qty).abs() > live_qty_epsilon(live.qty) {
                            info!(
                                "[{}] syncing tracked size {:.6} -> {:.6}",
                                symbol, tracked_mut.qty, live.qty
                            );
                            tracked_mut.qty = live.qty;
                            tracked_mut.entry_notional_usd = tracked_mut.entry_price * live.qty;
                            changed = true;
                            trade_fill_sync_needed = true;
                        }
                        if live.entry_price > 0.0 {
                            if (live.entry_price - tracked_mut.entry_price).abs()
                                > tracked_mut.entry_price.abs().max(1.0) * 1e-8
                            {
                                trade_fill_sync_needed = true;
                            }
                            tracked_mut.entry_price = live.entry_price;
                            tracked_mut.entry_notional_usd = live.entry_price * tracked_mut.qty;
                        }
                        if trade_fill_sync_needed {
                            if let Some(trade_id) = existing_trade_id {
                                self.journal.confirm_entry_fill(
                                    trade_id,
                                    tracked_mut.entry_price,
                                    tracked_mut.qty,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        for symbol in remove_symbols {
            self.state.positions.remove(&symbol);
        }

        let unmanaged: Vec<LivePosition> = live_positions
            .iter()
            .filter_map(|(symbol, position)| {
                if self.state.positions.contains_key(symbol) {
                    None
                } else {
                    Some(position.clone())
                }
            })
            .collect();

        for live in unmanaged {
            let entry_signal = signals
                .get(&live.symbol)
                .cloned()
                .unwrap_or_else(|| neutral_signal_from_live(&live));
            let trade_id = if let Some(open_trade) = self.journal.find_open_trade(&live.symbol)? {
                if open_trade.status == "PENDING_ENTRY" {
                    self.journal
                        .confirm_entry_fill(open_trade.id, live.entry_price, live.qty)?;
                }
                self.journal.log_event(
                    Some(open_trade.id),
                    &live.symbol,
                    "TRACKING_REATTACHED",
                    "reattached tracking to existing open trade",
                    Some(&entry_signal),
                )?;
                Some(open_trade.id)
            } else {
                Some(self.journal.log_adopted_trade(
                    &live.symbol,
                    live.direction,
                    live.entry_price,
                    live.qty,
                    &entry_signal,
                    "adopted_unknown",
                    Some("discovered live position during sync"),
                )?)
            };
            let bucket_policy = market_meta
                .get(&live.symbol)
                .map(|meta| oi_bucket_policy(meta.oi_usd))
                .unwrap_or_default();
            let (stop_price, take_profit_price) = self
                .build_exit_levels(
                    &live.symbol,
                    &live.exchange_symbol,
                    live.direction,
                    live.entry_price,
                    &bucket_policy,
                )
                .await
                .unwrap_or((0.0, 0.0));
            let opened_at_ms =
                estimated_adopted_open_time(crate::now_ms(), bucket_policy.min_hold_hours);
            info!(
                "[{}] adopting unmanaged live {} qty={:.6} px={:.6}",
                live.symbol, live.direction, live.qty, live.entry_price
            );
            self.state.positions.insert(
                live.symbol.clone(),
                TrackedPosition {
                    symbol: live.symbol.clone(),
                    exchange_symbol: live.exchange_symbol.clone(),
                    direction: live.direction,
                    qty: live.qty,
                    entry_price: live.entry_price,
                    entry_notional_usd: live.entry_price * live.qty,
                    stop_price,
                    take_profit_price,
                    opened_at_ms,
                    entry_signal,
                    bucket_policy,
                    trade_id,
                    entry_source: "adopted".to_string(),
                    acp_sltp_last_submit_ms: 0,
                    acp_sltp_last_stop_price: 0.0,
                    acp_sltp_last_take_profit_price: 0.0,
                    pressure_exit_last_check_ms: 0,
                },
            );
            changed = true;
        }
        Ok(changed)
    }

    async fn process_signal_exits(
        &mut self,
        signals: &HashMap<String, CombinedSignal>,
    ) -> Result<()> {
        let now_ms = crate::now_ms();
        let symbols: Vec<String> = self.state.positions.keys().cloned().collect();

        for symbol in symbols {
            let Some(position) = self.state.positions.get(&symbol).cloned() else {
                continue;
            };
            let min_hold_ms = (position.bucket_policy.min_hold_hours * 3600.0 * 1000.0) as u64;
            if now_ms.saturating_sub(position.opened_at_ms) < min_hold_ms {
                continue;
            }
            let Some(snapshot) = self.state.dead_cap_snapshots.get(&symbol).cloned() else {
                warn!(
                    "[{}] pressure check skipped: missing dead-cap snapshot",
                    symbol
                );
                continue;
            };
            if position.pressure_exit_last_check_ms > 0
                && now_ms.saturating_sub(position.pressure_exit_last_check_ms)
                    < PRESSURE_DECAY_CHECK_INTERVAL_MS
            {
                continue;
            }
            if pressure_exit_skips_coverage_low(&snapshot) {
                info!("[{}] pressure check skipped: {}", symbol, snapshot.reason);
                if let Some(tracked) = self.state.positions.get_mut(&symbol) {
                    tracked.pressure_exit_last_check_ms = now_ms;
                }
                continue;
            }
            let Some((pressure, threshold, cutoff)) =
                pressure_exit_metrics(&position, &snapshot, PRESSURE_DECAY_THRESHOLD_RATIO)
            else {
                warn!(
                    "[{}] pressure check skipped: invalid metrics signal={} strength={:.2} thr={:.2} effL={:.2}% effS={:.2}%",
                    symbol,
                    snapshot.signal,
                    snapshot.strength,
                    snapshot.threshold,
                    snapshot.effective_long_pct,
                    snapshot.effective_short_pct,
                );
                if let Some(tracked) = self.state.positions.get_mut(&symbol) {
                    tracked.pressure_exit_last_check_ms = now_ms;
                }
                continue;
            };

            let age_h = now_ms.saturating_sub(position.opened_at_ms) as f64 / 3_600_000.0;
            let monitored_side = match position.direction {
                Direction::Short => "effL",
                Direction::Long => "effS",
                Direction::Neutral => "eff?",
            };

            if pressure >= cutoff {
                info!(
                    "[{}] pressure check HOLD age={:.2}h side={} pressure={:.2}% threshold={:.2}% cutoff={:.2}% signal={} strength={:.2} {}",
                    symbol,
                    age_h,
                    monitored_side,
                    pressure,
                    threshold,
                    cutoff,
                    snapshot.signal,
                    snapshot.strength,
                    snapshot.reason
                );
                if let Some(tracked) = self.state.positions.get_mut(&symbol) {
                    tracked.pressure_exit_last_check_ms = now_ms;
                }
                continue;
            }

            let exit_signal = signals.get(&symbol);
            let reason = format!(
                "PRESSURE_DECAY {:.2}% < {:.2}% ({}% of {:.2}%)",
                pressure,
                cutoff,
                (PRESSURE_DECAY_THRESHOLD_RATIO * 100.0).round() as u64,
                threshold
            );
            info!(
                "[{}] pressure check EXIT age={:.2}h side={} pressure={:.2}% threshold={:.2}% cutoff={:.2}% signal={} strength={:.2} {}",
                symbol,
                age_h,
                monitored_side,
                pressure,
                threshold,
                cutoff,
                snapshot.signal,
                snapshot.strength,
                snapshot.reason
            );

            match self.exit_position(&position, &reason, exit_signal).await? {
                ExitDisposition::Closed => {
                    self.state.positions.remove(&symbol);
                    self.start_symbol_cooldown(&symbol, "PRESSURE_DECAY");
                }
                ExitDisposition::Detached => {
                    self.state.positions.remove(&symbol);
                }
                ExitDisposition::KeepTracking => {}
            }
        }
        Ok(())
    }

    async fn process_entries(
        &mut self,
        snapshot: &AccountDataSnapshot,
        signals: &HashMap<String, CombinedSignal>,
        live_summary: Option<&AccountSummary>,
        active_acp_symbols: &HashSet<String>,
    ) -> Result<()> {
        if let Some(summary) = live_summary {
            if summary.available_margin <= 0.0 {
                warn!(
                    "[ENTRY] skipping new entries: no available margin account_value={:.4} margin_used={:.4}",
                    summary.account_value,
                    summary.margin_used
                );
                return Ok(());
            }
        }

        let mut occupied_symbols = self.journal.open_trade_symbols()?;
        occupied_symbols.extend(active_acp_symbols.iter().cloned());
        let occupied_slots = occupied_symbols.len();
        let available_slots = self.cfg.max_open_positions.saturating_sub(occupied_slots);
        if available_slots == 0 {
            return Ok(());
        }

        let actionable_total = signals
            .values()
            .filter(|signal| signal.target_direction.is_actionable())
            .count();
        let now_ms = crate::now_ms();
        let cooldown_blocked = signals
            .values()
            .filter(|signal| signal.target_direction.is_actionable())
            .filter(|signal| !self.state.positions.contains_key(&signal.symbol))
            .filter(|signal| self.is_symbol_cooldown_active(&signal.symbol, now_ms))
            .count();

        let mut candidates = signals
            .values()
            .filter(|signal| signal.target_direction.is_actionable())
            .filter(|signal| !self.state.positions.contains_key(&signal.symbol))
            .filter(|signal| !active_acp_symbols.contains(&signal.symbol))
            .filter(|signal| !self.is_symbol_cooldown_active(&signal.symbol, now_ms))
            .filter(|signal| self.executor.is_supported_symbol(&signal.symbol))
            .filter_map(|signal| {
                snapshot
                    .market_meta
                    .get(&signal.symbol)
                    .filter(|meta| meta.mark_px > 0.0)
                    .map(|_| signal.clone())
            })
            .collect::<Vec<_>>();

        info!(
            "[ENTRY] actionable_signals={} eligible_candidates={} cooldown_blocked={} occupied_slots={} available_slots={}",
            actionable_total,
            candidates.len(),
            cooldown_blocked,
            occupied_slots,
            available_slots
        );

        candidates.sort_by(|left, right| {
            right
                .net_score
                .abs()
                .partial_cmp(&left.net_score.abs())
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.symbol.cmp(&right.symbol))
        });

        let entry_cap = if self.acp_executor.is_some() {
            available_slots.min(self.cfg.acp_max_new_entries_per_cycle)
        } else {
            available_slots
        };

        for signal in candidates.into_iter().take(entry_cap) {
            if self.journal.find_open_trade(&signal.symbol)?.is_some()
                || active_acp_symbols.contains(&signal.symbol)
            {
                continue;
            }
            let Some(meta) = snapshot.market_meta.get(&signal.symbol) else {
                continue;
            };
            if meta.mark_px <= 0.0 {
                continue;
            }
            let bucket_policy = oi_bucket_policy(meta.oi_usd);

            let requested_qty = self
                .executor
                .round_size(signal.order_notional_usd / meta.mark_px, &signal.symbol);
            if requested_qty <= 0.0 {
                continue;
            }
            if self.cfg.dry_run {
                if !self
                    .passes_late_entry_guard(
                        &signal.symbol,
                        &signal.exchange_symbol,
                        signal.target_direction,
                        meta.mark_px,
                        &bucket_policy,
                    )
                    .await?
                {
                    continue;
                }
                info!(
                    "[{}] DRY RUN: Would {} {:.6} @ {:.6} notional={:.2} {}",
                    signal.symbol,
                    signal.target_direction,
                    requested_qty,
                    meta.mark_px,
                    requested_qty * meta.mark_px,
                    signal.reason
                );
                continue;
            }
            match self.enter_position(meta, signal, requested_qty).await? {
                EntryAttemptOutcome::Entered
                | EntryAttemptOutcome::Pending
                | EntryAttemptOutcome::Rejected => {}
                EntryAttemptOutcome::MarginExhausted => break,
            }
            if self.acp_executor.is_some() && self.cfg.acp_entry_submit_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.cfg.acp_entry_submit_delay_ms)).await;
            }
        }
        Ok(())
    }

    async fn enter_position(
        &mut self,
        meta: &MarketMeta,
        signal: CombinedSignal,
        requested_qty: f64,
    ) -> Result<EntryAttemptOutcome> {
        let bucket_policy = oi_bucket_policy(meta.oi_usd);
        let acp_limit_price = if self.acp_executor.is_some() {
            Some(
                self.build_acp_limit_price(
                    &signal.symbol,
                    &signal.exchange_symbol,
                    signal.target_direction,
                )
                .await?,
            )
        } else {
            None
        };
        let intended_entry_price = acp_limit_price.unwrap_or(meta.mark_px);
        if !self
            .passes_late_entry_guard(
                &signal.symbol,
                &signal.exchange_symbol,
                signal.target_direction,
                intended_entry_price,
                &bucket_policy,
            )
            .await?
        {
            return Ok(EntryAttemptOutcome::Rejected);
        }
        let (initial_stop, initial_tp) = self
            .build_exit_levels(
                &signal.symbol,
                &signal.exchange_symbol,
                signal.target_direction,
                intended_entry_price,
                &bucket_policy,
            )
            .await?;
        if initial_stop <= 0.0 || initial_tp <= 0.0 {
            warn!(
                "[{}] failed to build exit levels before entry; skipping",
                signal.symbol
            );
            return Ok(EntryAttemptOutcome::Rejected);
        }
        let trade_id = self.journal.log_entry_intent(
            &signal,
            signal.target_direction,
            intended_entry_price,
            requested_qty,
            "signal",
            Some("pre-order entry intent"),
        )?;

        let result = if let Some(acp) = self.acp_executor.as_ref() {
            acp.open_limit_order(
                &signal.exchange_symbol,
                signal.target_direction,
                signal.order_notional_usd,
                acp_limit_price.unwrap_or(meta.mark_px),
                Some(self.effective_acp_leverage(&signal.symbol)),
                Some(initial_stop),
                Some(initial_tp),
            )
            .await?
        } else {
            self.executor
                .place_market_order(
                    &signal.symbol,
                    signal.target_direction,
                    requested_qty,
                    false,
                )
                .await?
        };
        let live_after = if let Some(acp) = self.acp_executor.as_ref() {
            acp.fetch_positions().await?.remove(&signal.symbol)
        } else {
            self.executor.get_position(&signal.symbol).await?
        };
        let live_matches_signal = live_after
            .as_ref()
            .filter(|live| live.direction == signal.target_direction && live.qty > 0.0);
        if let Some(job_id) = result.order_id.as_deref() {
            self.journal
                .attach_acp_job(trade_id, job_id, Some("SUBMITTED"))?;
        }
        if !result.success {
            if let Some(err) = result.error {
                warn!(
                    "[{}] entry rejected side={} qty={:.6} err={}",
                    signal.symbol, signal.target_direction, requested_qty, err
                );
                if live_matches_signal.is_some() {
                    warn!(
                        "[{}] adopting live position after rejected entry result",
                        signal.symbol
                    );
                    self.journal.log_event(
                        Some(trade_id),
                        &signal.symbol,
                        "ENTRY_RECOVERED",
                        "live position existed after rejected entry result",
                        Some(&signal),
                    )?;
                } else if is_insufficient_margin_error(&err) {
                    self.journal
                        .mark_entry_failed(trade_id, "INSUFFICIENT_MARGIN")?;
                    return Ok(EntryAttemptOutcome::MarginExhausted);
                }
            }
            if live_matches_signal.is_none() {
                self.journal.mark_entry_failed(trade_id, "ORDER_FAILED")?;
                return Ok(EntryAttemptOutcome::Rejected);
            }
        }

        let fill_size = live_matches_signal
            .map(|live| live.qty)
            .unwrap_or(result.filled_size);
        if self.acp_executor.is_some() && fill_size <= 0.0 {
            self.capture_pending_hl_order_id(trade_id, &signal.symbol, Some(intended_entry_price))
                .await?;
            self.journal.log_event(
                Some(trade_id),
                &signal.symbol,
                "ENTRY_PENDING",
                &format!(
                    "ACP limit order accepted; awaiting live fill job_id={}",
                    result.order_id.as_deref().unwrap_or("unknown")
                ),
                Some(&signal),
            )?;
            return Ok(EntryAttemptOutcome::Pending);
        }
        if fill_size <= 0.0 {
            warn!(
                "[{}] entry returned success but no live fill was detected; skipping tracking",
                signal.symbol
            );
            return Ok(EntryAttemptOutcome::Rejected);
        }
        let fill_price = live_matches_signal
            .map(|live| live.entry_price)
            .filter(|price| *price > 0.0)
            .unwrap_or_else(|| result.filled_price.max(meta.mark_px));
        self.journal
            .confirm_entry_fill(trade_id, fill_price, fill_size)?;

        let (stop_price, take_profit_price) = self
            .build_exit_levels(
                &signal.symbol,
                &signal.exchange_symbol,
                signal.target_direction,
                fill_price,
                &bucket_policy,
            )
            .await?;
        let exit_side = opposite_direction(signal.target_direction);

        let mut acp_sltp_last_submit_ms = 0;
        let mut acp_sltp_last_stop_price = 0.0;
        let mut acp_sltp_last_take_profit_price = 0.0;
        if let Some(acp) = self.acp_executor.as_ref() {
            let modify = acp
                .modify_position(
                    &signal.exchange_symbol,
                    None,
                    Some(stop_price),
                    if fill_size * take_profit_price >= self.cfg.tp_min_quote_usd {
                        Some(take_profit_price)
                    } else {
                        None
                    },
                )
                .await?;
            if !modify.success {
                warn!(
                    "[{}] failed to modify ACP SL/TP stop={:.6} tp={:.6}: {:?}",
                    signal.symbol, stop_price, take_profit_price, modify.error
                );
            } else {
                acp_sltp_last_submit_ms = crate::now_ms();
                acp_sltp_last_stop_price = stop_price;
                acp_sltp_last_take_profit_price =
                    if fill_size * take_profit_price >= self.cfg.tp_min_quote_usd {
                        take_profit_price
                    } else {
                        0.0
                    };
            }
        } else {
            let sl_result = self
                .executor
                .place_stop_order(&signal.symbol, exit_side, fill_size, stop_price, "sl")
                .await?;
            if !sl_result.success {
                warn!(
                    "[{}] failed to place SL @ {:.6}: {:?}",
                    signal.symbol, stop_price, sl_result.error
                );
            }
            if self.cfg.sltp_throttle_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.cfg.sltp_throttle_delay_ms)).await;
            }

            if fill_size * take_profit_price >= self.cfg.tp_min_quote_usd {
                let tp_result = self
                    .executor
                    .place_reduce_only_limit_order(
                        &signal.symbol,
                        exit_side,
                        fill_size,
                        take_profit_price,
                    )
                    .await?;
                if !tp_result.success {
                    warn!(
                        "[{}] failed to place TP @ {:.6}: {:?}",
                        signal.symbol, take_profit_price, tp_result.error
                    );
                }
            } else {
                warn!(
                    "[{}] TP skipped: quote {:.2} < min {:.2}",
                    signal.symbol,
                    fill_size * take_profit_price,
                    self.cfg.tp_min_quote_usd
                );
            }
        }

        self.state.positions.insert(
            signal.symbol.clone(),
            TrackedPosition {
                symbol: signal.symbol.clone(),
                exchange_symbol: signal.exchange_symbol.clone(),
                direction: signal.target_direction,
                qty: fill_size,
                entry_price: fill_price,
                entry_notional_usd: fill_size * fill_price,
                stop_price,
                take_profit_price,
                opened_at_ms: crate::now_ms(),
                entry_signal: signal.clone(),
                bucket_policy,
                trade_id: Some(trade_id),
                entry_source: "signal".to_string(),
                acp_sltp_last_submit_ms,
                acp_sltp_last_stop_price,
                acp_sltp_last_take_profit_price,
                pressure_exit_last_check_ms: 0,
            },
        );
        info!(
            "[{}] ENTER {} qty={:.6} px={:.6} notional={:.2} stop={:.6} tp={:.6} {}",
            signal.symbol,
            signal.target_direction,
            fill_size,
            fill_price,
            fill_size * fill_price,
            stop_price,
            take_profit_price,
            signal.reason
        );
        Ok(EntryAttemptOutcome::Entered)
    }

    async fn passes_late_entry_guard(
        &self,
        symbol: &str,
        exchange_symbol: &str,
        direction: Direction,
        entry_price: f64,
        bucket_policy: &OiBucketPolicy,
    ) -> Result<bool> {
        if !direction.is_actionable() || entry_price <= 0.0 {
            return Ok(true);
        }
        let checkpoint_data = match self
            .atr_client
            .fetch_checkpoint_data(&self.info_client, exchange_symbol)
            .await
        {
            Ok(data) => data,
            Err(err) => {
                warn!("[{}] late-entry guard skipped: {err:#}", symbol);
                return Ok(true);
            }
        };
        let Some(metrics) =
            late_entry_guard_metrics(direction, entry_price, &checkpoint_data, bucket_policy)
        else {
            warn!(
                "[{}] late-entry guard skipped: invalid checkpoint data",
                symbol
            );
            return Ok(true);
        };
        let hit_4h = late_entry_guard_hit(metrics.extension_4h_atr, metrics.threshold_4h_atr);
        let hit_8h = late_entry_guard_hit(metrics.extension_8h_atr, metrics.threshold_8h_atr);
        let hit_12h = late_entry_guard_hit(metrics.extension_12h_atr, metrics.threshold_12h_atr);
        if !hit_4h && !hit_8h && !hit_12h {
            return Ok(true);
        }

        let mut hit_parts = Vec::with_capacity(3);
        if hit_4h {
            hit_parts.push("4h");
        }
        if hit_8h {
            hit_parts.push("8h");
        }
        if hit_12h {
            hit_parts.push("12h");
        }
        let hit_label = hit_parts.join(",");
        info!(
            "[{}] late-entry guard veto {} px={:.6} bucket={} ext4h={:.2}/{:.2} ext8h={:.2}/{:.2} ext12h={:.2}/{:.2} hit={}",
            symbol,
            direction,
            entry_price,
            bucket_policy.bucket_label,
            metrics.extension_4h_atr,
            metrics.threshold_4h_atr,
            metrics.extension_8h_atr,
            metrics.threshold_8h_atr,
            metrics.extension_12h_atr,
            metrics.threshold_12h_atr,
            hit_label
        );
        Ok(false)
    }

    async fn exit_position(
        &mut self,
        position: &TrackedPosition,
        reason: &str,
        exit_signal: Option<&CombinedSignal>,
    ) -> Result<ExitDisposition> {
        if self.acp_executor.is_none() {
            self.cancel_symbol_sltp_orders(position).await?;
        }
        let close_side = opposite_direction(position.direction);
        let result = if let Some(acp) = self.acp_executor.as_ref() {
            acp.close_position(&position.exchange_symbol).await?
        } else {
            self.executor
                .place_market_order(&position.symbol, close_side, position.qty, true)
                .await?
        };
        if !result.success {
            warn!(
                "[{}] exit rejected side={} qty={:.6} err={:?}",
                position.symbol, close_side, position.qty, result.error
            );
            let closed = self
                .verify_flat_or_dust(&position.symbol, close_side, position.qty)
                .await?;
            if !closed {
                if let Some(trade_id) = position.trade_id.or(self
                    .journal
                    .find_open_trade(&position.symbol)?
                    .map(|trade| trade.id))
                {
                    self.journal.log_event(
                        Some(trade_id),
                        &position.symbol,
                        "EXIT_RETRY",
                        reason,
                        exit_signal,
                    )?;
                }
                return Ok(ExitDisposition::KeepTracking);
            }

            let mut exit_price = position.entry_price;
            if result.filled_price > 0.0 {
                exit_price = result.filled_price;
            } else if self.acp_executor.is_none() {
                let now_ms = crate::now_ms();
                let recent_fills = self
                    .info_client
                    .fetch_user_fills(Some(now_ms.saturating_sub(DISAPPEARED_FILL_LOOKBACK_MS)))
                    .await
                    .unwrap_or_default();
                if let Some(fill_price) =
                    recent_disappearance_fill_price(position, &recent_fills, now_ms)
                {
                    exit_price = fill_price;
                }
            }

            info!(
                "[{}] EXIT {} qty={:.6} px={:.6} reason={} (after rejected close)",
                position.symbol, close_side, position.qty, exit_price, reason
            );
            if let Some(trade_id) = position.trade_id.or(self
                .journal
                .find_open_trade(&position.symbol)?
                .map(|trade| trade.id))
            {
                let _ = self
                    .journal
                    .close_trade(trade_id, exit_price, reason, exit_signal)?;
            }
            return Ok(ExitDisposition::Closed);
        }
        let closed = self
            .verify_flat_or_dust(&position.symbol, close_side, position.qty)
            .await?;
        if !closed {
            warn!(
                "[{}] exit incomplete after retries; keeping tracked state",
                position.symbol
            );
            return Ok(ExitDisposition::KeepTracking);
        }

        let exit_price = if result.filled_price > 0.0 {
            result.filled_price
        } else {
            position.entry_price
        };
        info!(
            "[{}] EXIT {} qty={:.6} px={:.6} reason={}",
            position.symbol, close_side, position.qty, exit_price, reason
        );
        if let Some(trade_id) = position.trade_id.or(self
            .journal
            .find_open_trade(&position.symbol)?
            .map(|trade| trade.id))
        {
            let _ = self
                .journal
                .close_trade(trade_id, exit_price, reason, exit_signal)?;
        }
        Ok(ExitDisposition::Closed)
    }

    async fn verify_flat_or_dust(
        &mut self,
        symbol: &str,
        close_side: Direction,
        fallback_qty: f64,
    ) -> Result<bool> {
        for _ in 0..self.cfg.exit_verify_retries {
            if self.cfg.exit_verify_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.cfg.exit_verify_delay_ms)).await;
            }
            let live = if let Some(acp) = self.acp_executor.as_ref() {
                acp.fetch_positions().await?.remove(symbol)
            } else {
                self.executor.get_position(symbol).await?
            };
            match live {
                None => return Ok(true),
                Some(position) if position.is_flat() => return Ok(true),
                Some(position) => {
                    let min_size = self.executor.min_size(symbol);
                    if position.qty < size_epsilon(position.qty) || position.qty < min_size {
                        return Ok(true);
                    }

                    let retry = if let Some(acp) = self.acp_executor.as_ref() {
                        acp.close_position(&position.exchange_symbol).await?
                    } else {
                        self.executor
                            .place_market_order(
                                symbol,
                                close_side,
                                position.qty.max(fallback_qty),
                                true,
                            )
                            .await?
                    };
                    if !retry.success {
                        warn!(
                            "[{}] residual close rejected qty={:.6} err={:?}",
                            symbol, position.qty, retry.error
                        );
                    }
                }
            }
        }
        Ok(false)
    }

    async fn cancel_symbol_sltp_orders(&mut self, position: &TrackedPosition) -> Result<()> {
        if self.acp_executor.is_some() {
            return Ok(());
        }
        let open_orders = self
            .executor
            .get_open_orders(Some(&position.symbol))
            .await?;
        let order_ids = select_sltp_order_ids(position, &open_orders);
        for order in open_orders {
            if !order_ids.contains(&order.order_id) {
                continue;
            }
            let _ = self
                .executor
                .cancel_order(&position.symbol, &order.order_id)
                .await?;
        }
        Ok(())
    }

    async fn reconcile_sltp_orders(&mut self, _active_acp_jobs: &[AcpJobStatus]) -> Result<()> {
        if self.cfg.dry_run {
            return Ok(());
        }

        let positions: Vec<TrackedPosition> = self.state.positions.values().cloned().collect();
        if self.acp_executor.is_some() {
            let now_ms = crate::now_ms();
            let cooldown_ms = self
                .cfg
                .acp_sltp_resubmit_cooldown_secs
                .saturating_mul(1_000);
            let refresh_ms = self.cfg.acp_sltp_refresh_secs.saturating_mul(1_000);
            for mut position in positions {
                let mut changed = false;
                if position.stop_price <= 0.0 || position.take_profit_price <= 0.0 {
                    let (stop_price, take_profit_price) = self
                        .build_exit_levels(
                            &position.symbol,
                            &position.exchange_symbol,
                            position.direction,
                            position.entry_price,
                            &position.bucket_policy,
                        )
                        .await?;
                    if stop_price > 0.0 && take_profit_price > 0.0 {
                        position.stop_price = stop_price;
                        position.take_profit_price = take_profit_price;
                        changed = true;
                    }
                }
                if changed {
                    self.state
                        .positions
                        .insert(position.symbol.clone(), position.clone());
                }
                let tp = if position.qty * position.take_profit_price >= self.cfg.tp_min_quote_usd {
                    Some(position.take_profit_price)
                } else {
                    None
                };
                if !should_submit_acp_sltp(
                    &position,
                    position.stop_price,
                    tp,
                    now_ms,
                    cooldown_ms,
                    refresh_ms,
                ) {
                    continue;
                }
                let Some(acp) = self.acp_executor.as_ref() else {
                    break;
                };
                let result = acp
                    .modify_position(
                        &position.exchange_symbol,
                        None,
                        Some(position.stop_price),
                        tp,
                    )
                    .await?;
                if !result.success {
                    warn!(
                        "[{}] ACP SL/TP reconcile failed stop={:.6} tp={:.6}: {:?}",
                        position.symbol,
                        position.stop_price,
                        position.take_profit_price,
                        result.error
                    );
                    continue;
                }
                if let Some(stored) = self.state.positions.get_mut(&position.symbol) {
                    stored.acp_sltp_last_submit_ms = now_ms;
                    stored.acp_sltp_last_stop_price = position.stop_price;
                    stored.acp_sltp_last_take_profit_price = tp.unwrap_or(0.0);
                }
            }
            return Ok(());
        }

        for mut position in positions {
            if self.cfg.sltp_throttle_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.cfg.sltp_throttle_delay_ms)).await;
            }
            if (position.stop_price <= 0.0 || position.take_profit_price <= 0.0)
                && self.ensure_exit_levels(&mut position).await?
            {
                self.state
                    .positions
                    .insert(position.symbol.clone(), position.clone());
            }
            let open_orders = self
                .executor
                .get_open_orders(Some(&position.symbol))
                .await?;
            let sltp_state = classify_sltp_orders(&position, &open_orders);
            let sl_ok = size_matches(position.qty, sltp_state.sl_qty);
            let tp_expected =
                position.qty * position.take_profit_price >= self.cfg.tp_min_quote_usd;
            let tp_ok = !tp_expected || size_matches(position.qty, sltp_state.tp_qty);
            if sl_ok && tp_ok {
                continue;
            }
            let exit_side = opposite_direction(position.direction);
            if !sl_ok {
                for order_id in &sltp_state.sl_order_ids {
                    let _ = self
                        .executor
                        .cancel_order(&position.symbol, order_id)
                        .await?;
                }
                let result = self
                    .executor
                    .place_stop_order(
                        &position.symbol,
                        exit_side,
                        position.qty,
                        position.stop_price,
                        "sl",
                    )
                    .await?;
                if !result.success {
                    warn!(
                        "[{}] reconcile failed to place SL @ {:.6}: {:?}",
                        position.symbol, position.stop_price, result.error
                    );
                }
            }
            if tp_expected && !tp_ok {
                for order_id in &sltp_state.tp_order_ids {
                    let _ = self
                        .executor
                        .cancel_order(&position.symbol, order_id)
                        .await?;
                }
                let result = self
                    .executor
                    .place_reduce_only_limit_order(
                        &position.symbol,
                        exit_side,
                        position.qty,
                        position.take_profit_price,
                    )
                    .await?;
                if !result.success {
                    warn!(
                        "[{}] reconcile failed to place TP @ {:.6}: {:?}",
                        position.symbol, position.take_profit_price, result.error
                    );
                }
            }
        }
        Ok(())
    }

    async fn ensure_exit_levels(&mut self, position: &mut TrackedPosition) -> Result<bool> {
        if position.stop_price > 0.0 && position.take_profit_price > 0.0 {
            return Ok(false);
        }
        let (stop_price, take_profit_price) = self
            .build_exit_levels(
                &position.symbol,
                &position.exchange_symbol,
                position.direction,
                position.entry_price,
                &position.bucket_policy,
            )
            .await?;
        if stop_price <= 0.0 || take_profit_price <= 0.0 {
            return Ok(false);
        }
        position.stop_price = stop_price;
        position.take_profit_price = take_profit_price;
        Ok(true)
    }

    async fn reconcile_fill_journal(&mut self) -> Result<()> {
        if self.cfg.dry_run {
            return Ok(());
        }
        let since_ms = self
            .journal
            .last_fill_time_ms()?
            .map(|value| value.saturating_sub(300_000) as u64);
        let fills = self.info_client.fetch_user_fills(since_ms).await?;
        for fill in fills {
            let trade_id = self
                .journal
                .find_trade_for_fill(&fill.symbol, fill.ts_ms as i64)?;
            let fill_type = classify_fill_type(trade_id.is_some(), &fill, &self.state.positions);
            if self.journal.log_fill(trade_id, &fill, fill_type)? {
                self.journal.log_event(
                    trade_id,
                    &fill.symbol,
                    "FILL_RECORDED",
                    &format!(
                        "{} fill @ {:.6} size={:.6}",
                        fill_type, fill.price, fill.size
                    ),
                    Some(&fill),
                )?;
            }
        }
        Ok(())
    }

    async fn build_exit_levels(
        &self,
        symbol: &str,
        exchange_symbol: &str,
        direction: Direction,
        entry_price: f64,
        bucket_policy: &OiBucketPolicy,
    ) -> Result<(f64, f64)> {
        if !direction.is_actionable() || entry_price <= 0.0 {
            return Ok((0.0, 0.0));
        }
        let distance_pair = match self
            .atr_client
            .fetch(&self.info_client, exchange_symbol)
            .await
        {
            Ok(atr) if atr.atr > 0.0 => (
                atr.atr * bucket_policy.sl_atr_multiplier,
                atr.atr * bucket_policy.tp_atr_multiplier,
            ),
            Ok(_) => {
                let fallback = entry_price * self.cfg.fallback_exit_pct;
                warn!(
                    "[{}] ATR returned zero; using fallback {:.2}% exit distance",
                    symbol,
                    self.cfg.fallback_exit_pct * 100.0
                );
                (fallback, fallback)
            }
            Err(err) => {
                let fallback = entry_price * self.cfg.fallback_exit_pct;
                warn!(
                    "[{}] ATR unavailable ({}); using fallback {:.2}% exit distance",
                    symbol,
                    err,
                    self.cfg.fallback_exit_pct * 100.0
                );
                (fallback, fallback)
            }
        };
        let levels = match direction {
            Direction::Long => (
                self.executor
                    .round_price(entry_price - distance_pair.0, symbol),
                self.executor
                    .round_price(entry_price + distance_pair.1, symbol),
            ),
            Direction::Short => (
                self.executor
                    .round_price(entry_price + distance_pair.0, symbol),
                self.executor
                    .round_price(entry_price - distance_pair.1, symbol),
            ),
            Direction::Neutral => (0.0, 0.0),
        };
        Ok(levels)
    }

    fn log_summary(&self, context: &str) -> Result<()> {
        let summary = self.journal.summary()?;
        info!(
            "[{}] journal summary trades={} open={} closed={} pnl={:+.2}",
            context,
            summary.total_trades,
            summary.open_trades,
            summary.closed_trades,
            summary.total_pnl
        );
        Ok(())
    }

    async fn reconcile_acp_pending_entries(
        &mut self,
        signals: &HashMap<String, CombinedSignal>,
        live_positions: &HashMap<String, LivePosition>,
        active_acp_symbols: &HashSet<String>,
    ) -> Result<()> {
        if self.acp_executor.is_none() {
            let timeout_secs = self.cfg.chase_limit_timeout_secs + self.cfg.loop_interval_secs;
            let cutoff_ms = crate::now_ms().saturating_sub(timeout_secs * 1000) as i64;
            for pending in self.journal.pending_entries_before(cutoff_ms)? {
                if live_positions.contains_key(&pending.symbol)
                    || self.state.positions.contains_key(&pending.symbol)
                    || active_acp_symbols.contains(&pending.symbol)
                {
                    continue;
                }
                self.journal
                    .mark_entry_failed(pending.id, "ENTRY_TIMEOUT")?;
                self.journal.log_event(
                    Some(pending.id),
                    &pending.symbol,
                    "ENTRY_TIMEOUT",
                    "pending entry expired without live position",
                    None::<&CombinedSignal>,
                )?;
            }
            return Ok(());
        }

        let cutoff_ms =
            crate::now_ms().saturating_sub(self.cfg.acp_pending_timeout_secs * 1000) as i64;
        for pending in self.journal.pending_entries()? {
            if live_positions.contains_key(&pending.symbol)
                || self.state.positions.contains_key(&pending.symbol)
            {
                continue;
            }

            let hl_order_id = self
                .capture_pending_hl_order_id(pending.id, &pending.symbol, Some(pending.entry_price))
                .await?;
            let cancel_reason = pending_limit_cancel_reason(&pending, signals, cutoff_ms);
            if let Some(reason) = cancel_reason.as_deref() {
                if let Some(order_id) = hl_order_id.as_deref() {
                    let exchange_symbol = self.executor.exchange_symbol_for(&pending.symbol);
                    let result = self
                        .acp_executor
                        .as_ref()
                        .expect("ACP executor present")
                        .cancel_limit_order(&exchange_symbol, order_id)
                        .await?;
                    if !result.success {
                        warn!(
                            "[{}] failed to submit cancel_limit for oid={} reason={} err={:?}",
                            pending.symbol, order_id, reason, result.error
                        );
                        continue;
                    }
                    let cleared = self
                        .wait_for_hl_order_clear(&pending.symbol, order_id)
                        .await?;
                    if !cleared {
                        warn!(
                            "[{}] cancel_limit submitted for oid={} but order still visible",
                            pending.symbol, order_id
                        );
                        continue;
                    }
                    if self.executor.get_position(&pending.symbol).await?.is_some() {
                        self.journal.log_event(
                            Some(pending.id),
                            &pending.symbol,
                            "ENTRY_CANCEL_RACE",
                            &format!(
                                "cancel_limit oid={} cleared after live position appeared; keeping pending trade attached",
                                order_id
                            ),
                            None::<&CombinedSignal>,
                        )?;
                        continue;
                    }
                    self.journal.mark_entry_failed(pending.id, reason)?;
                    self.journal.log_event(
                        Some(pending.id),
                        &pending.symbol,
                        "ENTRY_CANCELED",
                        &format!("cancel_limit oid={} reason={}", order_id, reason),
                        None::<&CombinedSignal>,
                    )?;
                    continue;
                }
                if !active_acp_symbols.contains(&pending.symbol) {
                    self.journal.mark_entry_failed(pending.id, reason)?;
                    self.journal.log_event(
                        Some(pending.id),
                        &pending.symbol,
                        "ENTRY_CANCELED",
                        &format!(
                            "no live HL oid to cancel; clearing pending trade reason={}",
                            reason
                        ),
                        None::<&CombinedSignal>,
                    )?;
                }
                continue;
            }

            if pending.entry_time_ms <= cutoff_ms
                && !active_acp_symbols.contains(&pending.symbol)
                && hl_order_id.is_none()
            {
                self.journal
                    .mark_entry_failed(pending.id, "ENTRY_TIMEOUT")?;
                self.journal.log_event(
                    Some(pending.id),
                    &pending.symbol,
                    "ENTRY_TIMEOUT",
                    "pending ACP entry expired without live position or HL order",
                    None::<&CombinedSignal>,
                )?;
            }
        }
        Ok(())
    }

    async fn capture_pending_hl_order_id(
        &self,
        trade_id: i64,
        symbol: &str,
        target_price: Option<f64>,
    ) -> Result<Option<String>> {
        let open_orders = self.executor.get_open_orders(Some(symbol)).await?;
        let candidate = select_pending_limit_order(&open_orders, target_price);
        if let Some(order) = candidate {
            self.journal.attach_hl_order_id(trade_id, &order.order_id)?;
            return Ok(Some(order.order_id.clone()));
        }
        Ok(None)
    }

    async fn wait_for_hl_order_clear(&self, symbol: &str, order_id: &str) -> Result<bool> {
        for _ in 0..10 {
            let open_orders = self.executor.get_open_orders(Some(symbol)).await?;
            let still_live = open_orders
                .iter()
                .any(|order| order.order_id == order_id && !order.reduce_only);
            if !still_live {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(1_000)).await;
        }
        Ok(false)
    }

    async fn build_acp_limit_price(
        &self,
        symbol: &str,
        exchange_symbol: &str,
        direction: Direction,
    ) -> Result<f64> {
        let acp = self
            .acp_executor
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ACP executor unavailable"))?;
        let top_of_book = acp
            .fetch_top_of_book_price(exchange_symbol, direction)
            .await?;
        let multiplier = match direction {
            Direction::Long => 1.0 - (self.cfg.acp_limit_offset_bps / 10_000.0),
            Direction::Short => 1.0 + (self.cfg.acp_limit_offset_bps / 10_000.0),
            Direction::Neutral => 1.0,
        };
        let raw_price = top_of_book * multiplier;
        Ok(self.executor.round_price(raw_price, symbol))
    }

    fn effective_acp_leverage(&self, symbol: &str) -> f64 {
        self.cfg
            .acp_leverage
            .min(self.executor.max_leverage(symbol))
            .max(1.0)
    }

    async fn refresh_acp_pending_entries(
        &self,
        live_positions: &HashMap<String, LivePosition>,
        active_acp_jobs: &[AcpJobStatus],
    ) -> Result<()> {
        let Some(acp) = self.acp_executor.as_ref() else {
            return Ok(());
        };
        let job_map: HashMap<&str, &AcpJobStatus> = active_acp_jobs
            .iter()
            .filter_map(|job| job.job_id.as_deref().map(|id| (id, job)))
            .collect();
        for pending in self.journal.pending_entries()? {
            if live_positions.contains_key(&pending.symbol) {
                continue;
            }
            let Some(job_id) = pending.acp_job_id.as_deref() else {
                let orphan_cutoff_ms =
                    crate::now_ms().saturating_sub(self.cfg.loop_interval_secs * 2 * 1000) as i64;
                if pending.entry_time_ms <= orphan_cutoff_ms {
                    self.journal
                        .mark_entry_failed(pending.id, "MISSING_ACP_JOB_ID")?;
                    self.journal.log_event(
                        Some(pending.id),
                        &pending.symbol,
                        "ENTRY_REJECTED",
                        "pending ACP entry had no job id; cleared as orphaned intent",
                        None::<&CombinedSignal>,
                    )?;
                }
                continue;
            };
            let status = if let Some(status) = job_map.get(job_id) {
                (*status).clone()
            } else {
                match acp.get_job_status(job_id).await {
                    Ok(status) => status,
                    Err(err) => {
                        warn!(
                            "[{}] failed to refresh ACP job {}: {err:#}",
                            pending.symbol, job_id
                        );
                        continue;
                    }
                }
            };
            let phase = status.phase.trim();
            if pending.acp_phase.as_deref() != Some(phase) {
                self.journal.update_acp_phase(pending.id, Some(phase))?;
                self.journal.log_event(
                    Some(pending.id),
                    &pending.symbol,
                    "ACP_PHASE",
                    &format!("job_id={} phase={}", job_id, phase),
                    Some(&status.requirement),
                )?;
            }
            if let Some(requirement) = status
                .requirement
                .as_ref()
                .and_then(serde_json::Value::as_object)
            {
                let pair = requirement
                    .get("pair")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&pending.symbol);
                let canonical = crate::hyperliquid::normalize_hl_symbol(pair);
                let requested_leverage = requirement
                    .get("leverage")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let max_leverage = self.executor.max_leverage(&canonical);
                if requested_leverage > 0.0 && requested_leverage > max_leverage + f64::EPSILON {
                    let reason = format!(
                        "LEVERAGE_EXCEEDS_MAX requested={:.2} max={:.2}",
                        requested_leverage, max_leverage
                    );
                    self.journal.mark_entry_failed(pending.id, &reason)?;
                    self.journal.log_event(
                        Some(pending.id),
                        &pending.symbol,
                        "ENTRY_REJECTED",
                        &format!("job_id={} {}", job_id, reason),
                        Some(&status.requirement),
                    )?;
                    continue;
                }
            }
            if matches!(phase, "REJECTED" | "EXPIRED") {
                let reason = status
                    .rejection_reason
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(phase);
                self.journal.mark_entry_failed(pending.id, reason)?;
                self.journal.log_event(
                    Some(pending.id),
                    &pending.symbol,
                    "ENTRY_REJECTED",
                    &format!("job_id={} reason={}", job_id, reason),
                    Some(&status.requirement),
                )?;
            }
        }
        Ok(())
    }
}

fn state_path(state_dir: &Path) -> PathBuf {
    state_dir.join("state.json")
}

fn opposite_direction(direction: Direction) -> Direction {
    match direction {
        Direction::Long => Direction::Short,
        Direction::Short => Direction::Long,
        Direction::Neutral => Direction::Neutral,
    }
}

fn monitored_pressure_pct(position: &TrackedPosition, snapshot: &DeadCapSnapshot) -> Option<f64> {
    match position.direction {
        Direction::Short => Some(snapshot.effective_long_pct),
        Direction::Long => Some(snapshot.effective_short_pct),
        Direction::Neutral => None,
    }
}

fn pressure_exit_skips_coverage_low(snapshot: &DeadCapSnapshot) -> bool {
    snapshot.reason.starts_with("coverage low")
}

fn pressure_exit_metrics(
    position: &TrackedPosition,
    snapshot: &DeadCapSnapshot,
    threshold_ratio: f64,
) -> Option<(f64, f64, f64)> {
    if pressure_exit_skips_coverage_low(snapshot) {
        return None;
    }
    let pressure = monitored_pressure_pct(position, snapshot)?;
    let threshold = snapshot.threshold;
    if !pressure.is_finite()
        || threshold <= 0.0
        || !threshold.is_finite()
        || !threshold_ratio.is_finite()
        || threshold_ratio <= 0.0
    {
        return None;
    }
    let cutoff = threshold * threshold_ratio;
    Some((pressure, threshold, cutoff))
}

fn live_qty_epsilon(qty: f64) -> f64 {
    qty.abs().max(1.0) * 1e-6
}

fn is_insufficient_margin_error(err: &str) -> bool {
    err.to_ascii_lowercase().contains("insufficient margin")
}

fn active_acp_open_symbols(
    active_jobs: &[AcpJobStatus],
    executor: &crate::hyperliquid::ExecutionClient,
) -> HashSet<String> {
    let mut out = HashSet::new();
    for job in active_jobs {
        if job
            .rejection_reason
            .as_deref()
            .is_some_and(|reason| !reason.trim().is_empty())
        {
            continue;
        }
        if job.name.as_deref() != Some("perp_trade") {
            continue;
        }
        let requirement = job
            .requirement
            .as_ref()
            .and_then(serde_json::Value::as_object);
        let action = requirement
            .and_then(|req| req.get("action"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if action != "open" {
            continue;
        }
        let pair = requirement
            .and_then(|req| req.get("pair"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if pair.is_empty() {
            continue;
        }
        let symbol = crate::hyperliquid::normalize_hl_symbol(pair);
        let requested_leverage = requirement
            .and_then(|req| req.get("leverage"))
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        let max_leverage = executor.max_leverage(&symbol);
        if requested_leverage > 0.0 && requested_leverage > max_leverage + f64::EPSILON {
            continue;
        }
        out.insert(symbol);
    }
    out
}

fn pending_limit_cancel_reason(
    pending: &TradeRecord,
    signals: &HashMap<String, CombinedSignal>,
    stale_cutoff_ms: i64,
) -> Option<String> {
    if pending.entry_time_ms <= stale_cutoff_ms {
        return Some("ENTRY_TIMEOUT".to_string());
    }
    let target_direction = signals
        .get(&pending.symbol)
        .map(|signal| signal.target_direction)
        .unwrap_or(Direction::Neutral);
    if !target_direction.is_actionable() {
        return Some("SIGNAL_NEUTRAL".to_string());
    }
    if target_direction.opposes(pending.direction) {
        return Some(format!(
            "SIGNAL_FLIP {}->{}",
            pending.direction, target_direction
        ));
    }
    None
}

fn select_pending_limit_order(
    orders: &[OpenOrder],
    target_price: Option<f64>,
) -> Option<&OpenOrder> {
    let mut candidates: Vec<&OpenOrder> = orders
        .iter()
        .filter(|order| !order.reduce_only && order.order_type == "limit")
        .collect();
    if candidates.is_empty() {
        return None;
    }
    if let Some(target_price) = target_price.filter(|price| *price > 0.0) {
        candidates.sort_by(|left, right| {
            let left_dist = (left.price - target_price).abs();
            let right_dist = (right.price - target_price).abs();
            left_dist
                .partial_cmp(&right_dist)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.order_id.cmp(&right.order_id))
        });
        return candidates.into_iter().next();
    }
    candidates.sort_by(|left, right| left.order_id.cmp(&right.order_id));
    candidates.into_iter().next()
}

#[cfg(test)]
fn infer_sltp_presence(position: &TrackedPosition, orders: &[OpenOrder]) -> (bool, bool) {
    let classified = classify_sltp_orders(position, orders);
    (
        !classified.sl_order_ids.is_empty(),
        !classified.tp_order_ids.is_empty(),
    )
}

fn should_submit_acp_sltp(
    position: &TrackedPosition,
    desired_stop: f64,
    desired_take_profit: Option<f64>,
    now_ms: u64,
    cooldown_ms: u64,
    refresh_ms: u64,
) -> bool {
    let desired_tp = desired_take_profit.unwrap_or(0.0);
    if position.acp_sltp_last_submit_ms == 0 {
        return true;
    }
    let levels_changed = !sltp_price_matches(position.acp_sltp_last_stop_price, desired_stop)
        || !sltp_price_matches(position.acp_sltp_last_take_profit_price, desired_tp);
    let since_last_submit = now_ms.saturating_sub(position.acp_sltp_last_submit_ms);
    if levels_changed {
        return since_last_submit >= cooldown_ms;
    }
    since_last_submit >= refresh_ms
}

fn sltp_price_matches(left: f64, right: f64) -> bool {
    let tolerance = left.abs().max(right.abs()).max(1.0) * 1e-8;
    (left - right).abs() <= tolerance
}

fn select_sltp_order_ids(_position: &TrackedPosition, orders: &[OpenOrder]) -> HashSet<String> {
    let classified = classify_sltp_orders(_position, orders);
    classified
        .sl_order_ids
        .into_iter()
        .chain(classified.tp_order_ids)
        .collect()
}

fn size_epsilon(qty: f64) -> f64 {
    qty.abs().max(1.0) * 1e-6
}

fn classify_disappeared_position(
    position: &TrackedPosition,
    mark_price: Option<f64>,
    recent_fill_price: Option<f64>,
    sl_atr_multiplier: f64,
    tp_atr_multiplier: f64,
) -> (f64, String) {
    let exit_price = recent_fill_price
        .or(mark_price)
        .filter(|price| *price > 0.0)
        .unwrap_or(position.entry_price);
    if position.stop_price <= 0.0 || position.take_profit_price <= 0.0 || exit_price <= 0.0 {
        return (exit_price, "EXTERNAL_CLOSE".to_string());
    }
    let atr_est = estimated_atr(position, sl_atr_multiplier, tp_atr_multiplier);
    let tolerance = atr_est * 0.001;
    match position.direction {
        Direction::Long if exit_price <= position.stop_price + tolerance => {
            (exit_price, "LIKELY_SL".to_string())
        }
        Direction::Long if exit_price >= position.take_profit_price - tolerance => {
            (exit_price, "LIKELY_TP".to_string())
        }
        Direction::Short if exit_price >= position.stop_price - tolerance => {
            (exit_price, "LIKELY_SL".to_string())
        }
        Direction::Short if exit_price <= position.take_profit_price + tolerance => {
            (exit_price, "LIKELY_TP".to_string())
        }
        _ => (exit_price, "EXTERNAL_CLOSE".to_string()),
    }
}

fn recent_disappearance_fill_price(
    position: &TrackedPosition,
    recent_fills: &[UserFill],
    now_ms: u64,
) -> Option<f64> {
    let close_side = opposite_direction(position.direction);
    let window_start = now_ms.saturating_sub(DISAPPEARED_FILL_LOOKBACK_MS);

    let mut candidates: Vec<&UserFill> = recent_fills
        .iter()
        .filter(|fill| fill.symbol == position.symbol)
        .filter(|fill| fill.ts_ms >= window_start && fill.ts_ms <= now_ms)
        .filter(|fill| fill.side == close_side)
        .filter(|fill| fill_looks_like_close(fill, position.direction))
        .collect();

    if candidates.is_empty() {
        candidates = recent_fills
            .iter()
            .filter(|fill| fill.symbol == position.symbol)
            .filter(|fill| fill.ts_ms >= window_start && fill.ts_ms <= now_ms)
            .filter(|fill| fill.side == close_side)
            .collect();
    }

    let latest_ts = candidates.iter().map(|fill| fill.ts_ms).max()?;
    let cluster_start = latest_ts.saturating_sub(DISAPPEARED_FILL_CLUSTER_MS);
    let cluster = candidates
        .into_iter()
        .filter(|fill| fill.ts_ms >= cluster_start)
        .collect::<Vec<_>>();
    let total_size = cluster.iter().map(|fill| fill.size).sum::<f64>();
    if total_size <= 0.0 {
        return None;
    }
    Some(
        cluster
            .iter()
            .map(|fill| fill.price * fill.size)
            .sum::<f64>()
            / total_size,
    )
}

fn fill_looks_like_close(fill: &UserFill, position_direction: Direction) -> bool {
    if fill.side != opposite_direction(position_direction) {
        return false;
    }
    if let Some(dir_text) = fill.dir_text.as_deref() {
        let normalized = dir_text.trim().to_ascii_lowercase();
        if normalized.contains("close") {
            return true;
        }
    }
    if fill.closed_pnl.unwrap_or(0.0).abs() > 0.0 {
        return true;
    }
    fill.start_position
        .map(|start| start.abs() > 0.0)
        .unwrap_or(false)
}

fn estimated_atr(
    position: &TrackedPosition,
    sl_atr_multiplier: f64,
    tp_atr_multiplier: f64,
) -> f64 {
    let sl_component = if sl_atr_multiplier > 0.0 {
        (position.entry_price - position.stop_price).abs() / sl_atr_multiplier
    } else {
        0.0
    };
    let tp_component = if tp_atr_multiplier > 0.0 {
        (position.take_profit_price - position.entry_price).abs() / tp_atr_multiplier
    } else {
        0.0
    };
    sl_component
        .max(tp_component)
        .max(position.entry_price.abs() * 0.001)
}

fn late_entry_guard_metrics(
    direction: Direction,
    entry_price: f64,
    checkpoint_data: &AtrCheckpointData,
    bucket_policy: &OiBucketPolicy,
) -> Option<LateEntryGuardMetrics> {
    if entry_price <= 0.0 || checkpoint_data.atr <= 0.0 {
        return None;
    }
    let (anchor_4h, anchor_8h, anchor_12h) = match direction {
        Direction::Long => (
            checkpoint_data.low_4h_ago,
            checkpoint_data.low_8h_ago,
            checkpoint_data.low_12h_ago,
        ),
        Direction::Short => (
            checkpoint_data.high_4h_ago,
            checkpoint_data.high_8h_ago,
            checkpoint_data.high_12h_ago,
        ),
        Direction::Neutral => return None,
    };
    if anchor_4h <= 0.0 || anchor_8h <= 0.0 || anchor_12h <= 0.0 {
        return None;
    }
    let base_threshold = late_entry_guard_base_atr_multiplier(bucket_policy);
    Some(LateEntryGuardMetrics {
        extension_4h_atr: directional_entry_extension_atr(
            direction,
            entry_price,
            anchor_4h,
            checkpoint_data.atr,
        ),
        extension_8h_atr: directional_entry_extension_atr(
            direction,
            entry_price,
            anchor_8h,
            checkpoint_data.atr,
        ),
        extension_12h_atr: directional_entry_extension_atr(
            direction,
            entry_price,
            anchor_12h,
            checkpoint_data.atr,
        ),
        threshold_4h_atr: base_threshold,
        threshold_8h_atr: base_threshold + LATE_ENTRY_GUARD_8H_ATR_SURCHARGE,
        threshold_12h_atr: base_threshold + LATE_ENTRY_GUARD_12H_ATR_SURCHARGE,
    })
}

fn late_entry_guard_base_atr_multiplier(bucket_policy: &OiBucketPolicy) -> f64 {
    match bucket_policy.bucket_label.as_str() {
        ">=1B" => 2.0,
        ">=500M" => 2.25,
        ">=100M" => 2.5,
        ">=50M" => 2.75,
        _ => 3.0,
    }
}

fn directional_entry_extension_atr(
    direction: Direction,
    entry_price: f64,
    anchor_price: f64,
    atr: f64,
) -> f64 {
    match direction {
        Direction::Long => (entry_price - anchor_price) / atr,
        Direction::Short => (anchor_price - entry_price) / atr,
        Direction::Neutral => 0.0,
    }
}

fn late_entry_guard_hit(extension_atr: f64, threshold_atr: f64) -> bool {
    extension_atr >= threshold_atr
}

fn neutral_signal_from_live(live: &LivePosition) -> CombinedSignal {
    CombinedSignal {
        symbol: live.symbol.clone(),
        exchange_symbol: live.exchange_symbol.clone(),
        dead_cap: SignalComponent::neutral("unknown adopted live position"),
        whale: SignalComponent::neutral("unknown adopted live position"),
        net_score: 0.0,
        target_direction: Direction::Neutral,
        raw_notional_usd: 0.0,
        order_notional_usd: 0.0,
        reason: "adopted live position".to_string(),
    }
}

fn estimated_adopted_open_time(now_ms: u64, min_hold_hours: f64) -> u64 {
    let hold_ms = (min_hold_hours.max(0.0) * 3600.0 * 1000.0) as u64;
    now_ms.saturating_sub(hold_ms / 2)
}

fn classify_fill_type(
    matched_trade: bool,
    fill: &crate::types::UserFill,
    open_positions: &HashMap<String, TrackedPosition>,
) -> &'static str {
    if let Some(position) = open_positions.get(&fill.symbol) {
        if position.direction == fill.side {
            return "ENTRY";
        }
        return "EXIT";
    }
    if matched_trade {
        return "EXIT";
    }
    "UNMATCHED"
}

struct ClassifiedSltpOrders {
    sl_order_ids: HashSet<String>,
    tp_order_ids: HashSet<String>,
    sl_qty: f64,
    tp_qty: f64,
}

fn classify_sltp_orders(position: &TrackedPosition, orders: &[OpenOrder]) -> ClassifiedSltpOrders {
    let mut out = ClassifiedSltpOrders {
        sl_order_ids: HashSet::new(),
        tp_order_ids: HashSet::new(),
        sl_qty: 0.0,
        tp_qty: 0.0,
    };
    for order in orders {
        if !order.reduce_only {
            continue;
        }
        match order.order_type.as_str() {
            "sl" => {
                out.sl_qty += order.size;
                out.sl_order_ids.insert(order.order_id.clone());
            }
            "tp" => {
                out.tp_qty += order.size;
                out.tp_order_ids.insert(order.order_id.clone());
            }
            _ => {
                let is_stop = match position.direction {
                    Direction::Long => order.price <= position.entry_price,
                    Direction::Short => order.price >= position.entry_price,
                    Direction::Neutral => false,
                };
                if is_stop {
                    out.sl_qty += order.size;
                    out.sl_order_ids.insert(order.order_id.clone());
                } else {
                    out.tp_qty += order.size;
                    out.tp_order_ids.insert(order.order_id.clone());
                }
            }
        }
    }
    out
}

fn size_matches(expected: f64, actual: f64) -> bool {
    (expected - actual).abs() <= live_qty_epsilon(expected.max(actual))
}

pub fn combined_target(
    dead_cap: &SignalComponent,
    _whale: &SignalComponent,
    base_notional_usd: f64,
    min_trade_notional_usd: f64,
    bucket_policy: &OiBucketPolicy,
) -> (f64, Direction, f64, f64) {
    let scaled_base_notional_usd = base_notional_usd * bucket_policy.size_multiplier;
    let scaled_min_trade_notional_usd = min_trade_notional_usd * bucket_policy.size_multiplier;
    let net_score = dead_cap.direction.sign() * dead_cap.strength;
    let target_direction = Direction::from_score(net_score);
    let raw_notional_usd = scaled_base_notional_usd * net_score.abs();
    let order_notional_usd = if target_direction.is_actionable() {
        raw_notional_usd.max(scaled_min_trade_notional_usd)
    } else {
        0.0
    };
    (
        net_score,
        target_direction,
        raw_notional_usd,
        order_notional_usd,
    )
}

fn oi_bucket_policy(oi_usd: f64) -> OiBucketPolicy {
    if oi_usd >= 1_000_000_000.0 {
        OiBucketPolicy {
            bucket_label: ">=1B".to_string(),
            oi_usd_at_entry: oi_usd,
            size_multiplier: 3.0,
            sl_atr_multiplier: 2.0,
            tp_atr_multiplier: 3.0,
            min_hold_hours: 2.0,
        }
    } else if oi_usd >= 500_000_000.0 {
        OiBucketPolicy {
            bucket_label: ">=500M".to_string(),
            oi_usd_at_entry: oi_usd,
            size_multiplier: 2.5,
            sl_atr_multiplier: 2.25,
            tp_atr_multiplier: 3.5,
            min_hold_hours: 2.0,
        }
    } else if oi_usd >= 100_000_000.0 {
        OiBucketPolicy {
            bucket_label: ">=100M".to_string(),
            oi_usd_at_entry: oi_usd,
            size_multiplier: 2.0,
            sl_atr_multiplier: 2.5,
            tp_atr_multiplier: 4.0,
            min_hold_hours: 3.0,
        }
    } else if oi_usd >= 50_000_000.0 {
        OiBucketPolicy {
            bucket_label: ">=50M".to_string(),
            oi_usd_at_entry: oi_usd,
            size_multiplier: 1.5,
            sl_atr_multiplier: 2.75,
            tp_atr_multiplier: 4.25,
            min_hold_hours: 3.0,
        }
    } else if oi_usd >= 40_000_000.0 {
        OiBucketPolicy {
            bucket_label: ">=40M".to_string(),
            oi_usd_at_entry: oi_usd,
            size_multiplier: 1.0,
            sl_atr_multiplier: 3.0,
            tp_atr_multiplier: 4.75,
            min_hold_hours: 3.0,
        }
    } else if oi_usd >= 25_000_000.0 {
        OiBucketPolicy {
            bucket_label: ">=25M".to_string(),
            oi_usd_at_entry: oi_usd,
            size_multiplier: 0.8,
            sl_atr_multiplier: 3.25,
            tp_atr_multiplier: 5.0,
            min_hold_hours: 4.0,
        }
    } else if oi_usd >= 10_000_000.0 {
        OiBucketPolicy {
            bucket_label: ">=10M".to_string(),
            oi_usd_at_entry: oi_usd,
            size_multiplier: 0.6,
            sl_atr_multiplier: 3.5,
            tp_atr_multiplier: 6.0,
            min_hold_hours: 5.0,
        }
    } else {
        OiBucketPolicy {
            bucket_label: ">=1M".to_string(),
            oi_usd_at_entry: oi_usd,
            size_multiplier: 0.3,
            sl_atr_multiplier: 3.75,
            tp_atr_multiplier: 7.0,
            min_hold_hours: 6.0,
        }
    }
}

fn should_refresh_bucket_policy(bucket_policy: &OiBucketPolicy) -> bool {
    bucket_policy.bucket_label == "legacy" || !bucket_policy.is_initialized()
}

fn resolved_bucket_policy(
    current_policy: &OiBucketPolicy,
    fallback_policy: &OiBucketPolicy,
) -> OiBucketPolicy {
    if should_refresh_bucket_policy(current_policy) {
        fallback_policy.clone()
    } else {
        current_policy.clone()
    }
}

pub fn should_release_reentry_block(
    blocked_direction: Direction,
    target_direction: Direction,
) -> bool {
    target_direction == Direction::Neutral || target_direction.opposes(blocked_direction)
}

fn should_start_symbol_cooldown(exit_reason: &str) -> bool {
    matches!(exit_reason, "LIKELY_SL" | "LIKELY_TP" | "EXTERNAL_CLOSE")
}

#[cfg(test)]
mod tests {
    use super::{
        classify_disappeared_position, combined_target, infer_sltp_presence,
        late_entry_guard_base_atr_multiplier, late_entry_guard_hit, late_entry_guard_metrics,
        oi_bucket_policy, pressure_exit_metrics, recent_disappearance_fill_price,
        should_start_symbol_cooldown, should_submit_acp_sltp,
    };
    use crate::types::{
        AtrCheckpointData, CombinedSignal, DeadCapSnapshot, Direction, OiBucketPolicy, OpenOrder,
        SignalComponent, TrackedPosition, UserFill,
    };

    #[test]
    fn infer_sltp_presence_from_reduce_only_prices_for_short() {
        let position = TrackedPosition {
            symbol: "LINK".to_string(),
            exchange_symbol: "LINK".to_string(),
            direction: Direction::Short,
            qty: 1.6,
            entry_price: 8.60,
            entry_notional_usd: 13.76,
            stop_price: 8.77,
            take_profit_price: 8.34,
            opened_at_ms: 0,
            entry_signal: CombinedSignal {
                symbol: "LINK".to_string(),
                exchange_symbol: "LINK".to_string(),
                dead_cap: SignalComponent::neutral(""),
                whale: SignalComponent::neutral(""),
                net_score: -0.5,
                target_direction: Direction::Short,
                raw_notional_usd: 15.0,
                order_notional_usd: 15.0,
                reason: String::new(),
            },
            bucket_policy: OiBucketPolicy::default(),
            trade_id: None,
            entry_source: "signal".to_string(),
            acp_sltp_last_submit_ms: 0,
            acp_sltp_last_stop_price: 0.0,
            acp_sltp_last_take_profit_price: 0.0,
            pressure_exit_last_check_ms: 0,
        };
        let orders = vec![
            OpenOrder {
                order_id: "1".to_string(),
                order_type: "reduce_only".to_string(),
                reduce_only: true,
                price: 8.77,
                size: 1.6,
                symbol: "LINK".to_string(),
            },
            OpenOrder {
                order_id: "2".to_string(),
                order_type: "reduce_only".to_string(),
                reduce_only: true,
                price: 8.34,
                size: 1.6,
                symbol: "LINK".to_string(),
            },
        ];
        assert_eq!(infer_sltp_presence(&position, &orders), (true, true));
    }

    #[test]
    fn infer_sltp_presence_with_explicit_sl_and_reduce_only_tp_limit() {
        let position = TrackedPosition {
            symbol: "BTC".to_string(),
            exchange_symbol: "BTC".to_string(),
            direction: Direction::Long,
            qty: 0.5,
            entry_price: 100.0,
            entry_notional_usd: 50.0,
            stop_price: 95.0,
            take_profit_price: 110.0,
            opened_at_ms: 0,
            entry_signal: CombinedSignal {
                symbol: "BTC".to_string(),
                exchange_symbol: "BTC".to_string(),
                dead_cap: SignalComponent::neutral(""),
                whale: SignalComponent::neutral(""),
                net_score: 0.5,
                target_direction: Direction::Long,
                raw_notional_usd: 15.0,
                order_notional_usd: 15.0,
                reason: String::new(),
            },
            bucket_policy: OiBucketPolicy::default(),
            trade_id: None,
            entry_source: "signal".to_string(),
            acp_sltp_last_submit_ms: 0,
            acp_sltp_last_stop_price: 0.0,
            acp_sltp_last_take_profit_price: 0.0,
            pressure_exit_last_check_ms: 0,
        };
        let orders = vec![
            OpenOrder {
                order_id: "sl".to_string(),
                order_type: "sl".to_string(),
                reduce_only: true,
                price: 95.0,
                size: 0.5,
                symbol: "BTC".to_string(),
            },
            OpenOrder {
                order_id: "tp-limit".to_string(),
                order_type: "unknown_reduce_only".to_string(),
                reduce_only: true,
                price: 110.0,
                size: 0.5,
                symbol: "BTC".to_string(),
            },
        ];
        assert_eq!(infer_sltp_presence(&position, &orders), (true, true));
    }

    #[test]
    fn acp_sltp_submit_respects_cooldown_and_refresh() {
        let position = TrackedPosition {
            symbol: "BTC".to_string(),
            exchange_symbol: "BTC".to_string(),
            direction: Direction::Long,
            qty: 1.0,
            entry_price: 100.0,
            entry_notional_usd: 100.0,
            stop_price: 95.0,
            take_profit_price: 110.0,
            opened_at_ms: 0,
            entry_signal: CombinedSignal {
                symbol: "BTC".to_string(),
                exchange_symbol: "BTC".to_string(),
                dead_cap: SignalComponent::neutral(""),
                whale: SignalComponent::neutral(""),
                net_score: 0.5,
                target_direction: Direction::Long,
                raw_notional_usd: 15.0,
                order_notional_usd: 15.0,
                reason: String::new(),
            },
            bucket_policy: OiBucketPolicy::default(),
            trade_id: None,
            entry_source: "signal".to_string(),
            acp_sltp_last_submit_ms: 1_000,
            acp_sltp_last_stop_price: 95.0,
            acp_sltp_last_take_profit_price: 110.0,
            pressure_exit_last_check_ms: 0,
        };

        assert!(!should_submit_acp_sltp(
            &position,
            95.0,
            Some(110.0),
            10_000,
            15_000,
            60_000,
        ));
        assert!(should_submit_acp_sltp(
            &position,
            95.0,
            Some(110.0),
            70_000,
            15_000,
            60_000,
        ));
        assert!(!should_submit_acp_sltp(
            &position,
            94.0,
            Some(111.0),
            10_000,
            15_000,
            60_000,
        ));
        assert!(should_submit_acp_sltp(
            &position,
            94.0,
            Some(111.0),
            17_000,
            15_000,
            60_000,
        ));
    }

    #[test]
    fn symbol_cooldown_applies_only_to_tp_and_sl_disappearances() {
        assert!(should_start_symbol_cooldown("LIKELY_TP"));
        assert!(should_start_symbol_cooldown("LIKELY_SL"));
        assert!(should_start_symbol_cooldown("EXTERNAL_CLOSE"));
        assert!(!should_start_symbol_cooldown("dead_cap=LONG"));
    }

    #[test]
    fn oi_bucket_policy_thresholds_match_live_schedule() {
        let p = oi_bucket_policy(1_100_000_000.0);
        assert_eq!(p.bucket_label, ">=1B");
        assert_eq!(p.size_multiplier, 3.0);
        assert_eq!(p.sl_atr_multiplier, 2.0);
        assert_eq!(p.tp_atr_multiplier, 3.0);
        assert_eq!(p.min_hold_hours, 2.0);

        let p = oi_bucket_policy(600_000_000.0);
        assert_eq!(p.bucket_label, ">=500M");
        assert_eq!(p.size_multiplier, 2.5);

        let p = oi_bucket_policy(150_000_000.0);
        assert_eq!(p.bucket_label, ">=100M");
        assert_eq!(p.size_multiplier, 2.0);
        assert_eq!(p.min_hold_hours, 3.0);

        let p = oi_bucket_policy(60_000_000.0);
        assert_eq!(p.bucket_label, ">=50M");
        assert_eq!(p.size_multiplier, 1.5);
        assert_eq!(p.min_hold_hours, 3.0);

        let p = oi_bucket_policy(45_000_000.0);
        assert_eq!(p.bucket_label, ">=40M");
        assert_eq!(p.size_multiplier, 1.0);
        assert_eq!(p.min_hold_hours, 3.0);

        let p = oi_bucket_policy(30_000_000.0);
        assert_eq!(p.bucket_label, ">=25M");
        assert_eq!(p.size_multiplier, 0.8);
        assert_eq!(p.min_hold_hours, 4.0);

        let p = oi_bucket_policy(15_000_000.0);
        assert_eq!(p.bucket_label, ">=10M");
        assert_eq!(p.size_multiplier, 0.6);
        assert_eq!(p.min_hold_hours, 5.0);

        let p = oi_bucket_policy(5_000_000.0);
        assert_eq!(p.bucket_label, ">=1M");
        assert_eq!(p.size_multiplier, 0.3);
        assert_eq!(p.sl_atr_multiplier, 3.75);
        assert_eq!(p.tp_atr_multiplier, 7.0);
        assert_eq!(p.min_hold_hours, 6.0);
    }

    #[test]
    fn combined_target_uses_bucket_size_multiplier() {
        let dead_cap = SignalComponent {
            direction: Direction::Long,
            strength: 0.8,
            reason: String::new(),
        };
        let whale = SignalComponent::neutral("");
        let bucket_policy = oi_bucket_policy(1_200_000_000.0);
        let (_, target_direction, raw_notional_usd, order_notional_usd) =
            combined_target(&dead_cap, &whale, 500.0, 150.0, &bucket_policy);
        assert_eq!(target_direction, Direction::Long);
        assert!((raw_notional_usd - 1_200.0).abs() < 1e-9);
        assert!((order_notional_usd - 1_200.0).abs() < 1e-9);
    }

    #[test]
    fn recent_close_fill_reclassifies_disappearance_as_tp() {
        let position = TrackedPosition {
            symbol: "BTC".to_string(),
            exchange_symbol: "BTC".to_string(),
            direction: Direction::Long,
            qty: 1.0,
            entry_price: 100.0,
            entry_notional_usd: 100.0,
            stop_price: 95.0,
            take_profit_price: 110.0,
            opened_at_ms: 0,
            entry_signal: CombinedSignal {
                symbol: "BTC".to_string(),
                exchange_symbol: "BTC".to_string(),
                dead_cap: SignalComponent::neutral(""),
                whale: SignalComponent::neutral(""),
                net_score: 0.5,
                target_direction: Direction::Long,
                raw_notional_usd: 15.0,
                order_notional_usd: 15.0,
                reason: String::new(),
            },
            bucket_policy: OiBucketPolicy::default(),
            trade_id: None,
            entry_source: "signal".to_string(),
            acp_sltp_last_submit_ms: 0,
            acp_sltp_last_stop_price: 0.0,
            acp_sltp_last_take_profit_price: 0.0,
            pressure_exit_last_check_ms: 0,
        };
        let now_ms = 1_000_000;
        let fills = vec![UserFill {
            fill_id: "tid:1".to_string(),
            symbol: "BTC".to_string(),
            exchange_symbol: "BTC".to_string(),
            side: Direction::Short,
            dir_text: Some("Close Long".to_string()),
            price: 110.0,
            size: 1.0,
            ts_ms: now_ms - 1_000,
            oid: Some(1),
            tid: Some(1),
            start_position: Some(1.0),
            closed_pnl: Some(10.0),
            fee_usd: Some(0.1),
            builder_fee_usd: None,
            crossed: true,
            twap: false,
        }];

        let recent_fill_price = recent_disappearance_fill_price(&position, &fills, now_ms);
        let (exit_price, exit_reason) =
            classify_disappeared_position(&position, None, recent_fill_price, 1.5, 1.0);

        assert_eq!(recent_fill_price, Some(110.0));
        assert_eq!(exit_reason, "LIKELY_TP");
        assert_eq!(exit_price, 110.0);
    }

    #[test]
    fn pressure_exit_uses_directional_effective_pressure() {
        let position = TrackedPosition {
            symbol: "ETH".to_string(),
            exchange_symbol: "ETH".to_string(),
            direction: Direction::Short,
            qty: 1.0,
            entry_price: 100.0,
            entry_notional_usd: 100.0,
            stop_price: 105.0,
            take_profit_price: 95.0,
            opened_at_ms: 0,
            entry_signal: CombinedSignal {
                symbol: "ETH".to_string(),
                exchange_symbol: "ETH".to_string(),
                dead_cap: SignalComponent::neutral(""),
                whale: SignalComponent::neutral(""),
                net_score: -0.5,
                target_direction: Direction::Short,
                raw_notional_usd: 15.0,
                order_notional_usd: 15.0,
                reason: String::new(),
            },
            bucket_policy: OiBucketPolicy::default(),
            trade_id: None,
            entry_source: "signal".to_string(),
            acp_sltp_last_submit_ms: 0,
            acp_sltp_last_stop_price: 0.0,
            acp_sltp_last_take_profit_price: 0.0,
            pressure_exit_last_check_ms: 0,
        };
        let snapshot = DeadCapSnapshot {
            symbol: "ETH".to_string(),
            signal: Direction::Neutral,
            strength: 0.0,
            threshold: 6.0,
            locked_long_pct: 0.0,
            locked_short_pct: 0.0,
            effective_long_pct: 4.2,
            effective_short_pct: 1.0,
            bad_long_pct: 0.0,
            bad_short_pct: 0.0,
            smart_long_pct: 0.0,
            smart_short_pct: 0.0,
            observed_pct: 90.0,
            locked_wallet_count: 10,
            dominant_top_share: 50.0,
            persistence_streak: 0,
            reason: "test".to_string(),
        };

        let metrics = pressure_exit_metrics(&position, &snapshot, 0.8).expect("metrics");
        assert!((metrics.0 - 4.2).abs() < 1e-9);
        assert!((metrics.1 - 6.0).abs() < 1e-9);
        assert!((metrics.2 - 4.8).abs() < 1e-9);
    }

    #[test]
    fn pressure_exit_rejects_coverage_low_snapshot() {
        let position = TrackedPosition {
            symbol: "TRX".to_string(),
            exchange_symbol: "TRX".to_string(),
            direction: Direction::Short,
            qty: 10.0,
            entry_price: 1.0,
            entry_notional_usd: 10.0,
            stop_price: 1.1,
            take_profit_price: 0.9,
            opened_at_ms: 0,
            entry_signal: CombinedSignal {
                symbol: "TRX".to_string(),
                exchange_symbol: "TRX".to_string(),
                dead_cap: SignalComponent::neutral(""),
                whale: SignalComponent::neutral(""),
                net_score: -0.5,
                target_direction: Direction::Short,
                raw_notional_usd: 10.0,
                order_notional_usd: 10.0,
                reason: String::new(),
            },
            bucket_policy: OiBucketPolicy::default(),
            trade_id: None,
            entry_source: "signal".to_string(),
            acp_sltp_last_submit_ms: 0,
            acp_sltp_last_stop_price: 0.0,
            acp_sltp_last_take_profit_price: 0.0,
            pressure_exit_last_check_ms: 0,
        };
        let snapshot = DeadCapSnapshot {
            symbol: "TRX".to_string(),
            signal: Direction::Neutral,
            strength: 0.0,
            threshold: 12.0,
            locked_long_pct: 0.0,
            locked_short_pct: 0.0,
            effective_long_pct: 0.0,
            effective_short_pct: 0.0,
            bad_long_pct: 0.0,
            bad_short_pct: 0.0,
            smart_long_pct: 0.0,
            smart_short_pct: 0.0,
            observed_pct: 0.0,
            locked_wallet_count: 0,
            dominant_top_share: 0.0,
            persistence_streak: 0,
            reason: "coverage low".to_string(),
        };

        assert!(pressure_exit_metrics(&position, &snapshot, 0.8).is_none());
    }

    #[test]
    fn late_entry_guard_uses_bucket_base_schedule() {
        let major = oi_bucket_policy(1_100_000_000.0);
        assert!((late_entry_guard_base_atr_multiplier(&major) - 2.0).abs() < 1e-9);

        let mid = oi_bucket_policy(600_000_000.0);
        assert!((late_entry_guard_base_atr_multiplier(&mid) - 2.25).abs() < 1e-9);

        let small = oi_bucket_policy(15_000_000.0);
        assert!((late_entry_guard_base_atr_multiplier(&small) - 3.0).abs() < 1e-9);
    }

    #[test]
    fn late_entry_guard_allows_subthreshold_directional_short_extension() {
        let bucket = oi_bucket_policy(15_000_000.0);
        let checkpoint_data = AtrCheckpointData {
            atr: 1.0,
            high_4h_ago: 102.5,
            low_4h_ago: 98.5,
            high_8h_ago: 104.0,
            low_8h_ago: 97.5,
            high_12h_ago: 105.0,
            low_12h_ago: 96.5,
        };
        let metrics = late_entry_guard_metrics(Direction::Short, 100.0, &checkpoint_data, &bucket)
            .expect("metrics");
        assert!((metrics.threshold_4h_atr - 3.0).abs() < 1e-9);
        assert!((metrics.threshold_8h_atr - 5.0).abs() < 1e-9);
        assert!((metrics.threshold_12h_atr - 6.0).abs() < 1e-9);
        assert!((metrics.extension_4h_atr - 2.5).abs() < 1e-9);
        assert!(!late_entry_guard_hit(
            metrics.extension_4h_atr,
            metrics.threshold_4h_atr
        ));
        assert!(!late_entry_guard_hit(
            metrics.extension_8h_atr,
            metrics.threshold_8h_atr
        ));
        assert!(!late_entry_guard_hit(
            metrics.extension_12h_atr,
            metrics.threshold_12h_atr
        ));
    }

    #[test]
    fn late_entry_guard_blocks_short_on_four_hour_high_extension() {
        let bucket = oi_bucket_policy(600_000_000.0);
        let checkpoint_data = AtrCheckpointData {
            atr: 1.0,
            high_4h_ago: 103.0,
            low_4h_ago: 98.0,
            high_8h_ago: 103.8,
            low_8h_ago: 97.0,
            high_12h_ago: 104.5,
            low_12h_ago: 96.0,
        };
        let metrics = late_entry_guard_metrics(Direction::Short, 100.0, &checkpoint_data, &bucket)
            .expect("metrics");
        assert!((metrics.threshold_4h_atr - 2.25).abs() < 1e-9);
        assert!((metrics.threshold_8h_atr - 4.25).abs() < 1e-9);
        assert!((metrics.threshold_12h_atr - 5.25).abs() < 1e-9);
        assert!(late_entry_guard_hit(
            metrics.extension_4h_atr,
            metrics.threshold_4h_atr
        ));
        assert!(!late_entry_guard_hit(
            metrics.extension_8h_atr,
            metrics.threshold_8h_atr
        ));
        assert!(!late_entry_guard_hit(
            metrics.extension_12h_atr,
            metrics.threshold_12h_atr
        ));
    }

    #[test]
    fn late_entry_guard_blocks_long_on_directional_low_extension() {
        let bucket = oi_bucket_policy(1_200_000_000.0);
        let checkpoint_data = AtrCheckpointData {
            atr: 1.0,
            high_4h_ago: 101.0,
            low_4h_ago: 97.5,
            high_8h_ago: 102.0,
            low_8h_ago: 95.8,
            high_12h_ago: 103.0,
            low_12h_ago: 94.8,
        };
        let metrics = late_entry_guard_metrics(Direction::Long, 100.0, &checkpoint_data, &bucket)
            .expect("metrics");
        assert!((metrics.threshold_4h_atr - 2.0).abs() < 1e-9);
        assert!(late_entry_guard_hit(
            metrics.extension_4h_atr,
            metrics.threshold_4h_atr
        ));
        assert!(late_entry_guard_hit(
            metrics.extension_8h_atr,
            metrics.threshold_8h_atr
        ));
        assert!(late_entry_guard_hit(
            metrics.extension_12h_atr,
            metrics.threshold_12h_atr
        ));
    }
}
