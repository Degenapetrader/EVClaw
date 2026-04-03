use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use log::{info, warn};
use tokio::signal::unix::{signal, SignalKind};

use crate::atr::AtrClient;
use crate::config::Config;
use crate::hyperliquid::{ExecutionClient, InfoClient};
use crate::journal::TradeJournal;
use crate::labels::WalletLabelStore;
use crate::signals::dead_cap::DeadCapitalSignal;
use crate::signals::whale::WhaleSignal;
use crate::state::{RuntimeState, StateStore};
use crate::types::{
    AccountDataSnapshot, AccountSummary, CombinedSignal, Direction, LivePosition, MarketMeta,
    OpenOrder, SignalComponent, TrackedPosition,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryAttemptOutcome {
    Entered,
    Rejected,
    MarginExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitDisposition {
    Closed,
    Detached,
    KeepTracking,
}

pub struct EvClawRuntime {
    cfg: Config,
    labels: WalletLabelStore,
    info_client: InfoClient,
    executor: ExecutionClient,
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
        let state = state_store.load()?;
        let journal = TradeJournal::new(cfg.journal_path.clone())?;

        Ok(Self {
            info_client: InfoClient::new(&cfg)?,
            executor: ExecutionClient::new(&cfg).await?,
            atr_client: AtrClient::new(&cfg)?,
            journal,
            dead_cap: DeadCapitalSignal,
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
        let tracked_wallets = self.labels.tracked_wallets();

        let market_meta = self
            .info_client
            .fetch_market_meta(self.cfg.symbols.as_ref())
            .await?;
        self.executor.register_market_meta(&market_meta);
        if self.cycle_count == 1 {
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
        let live_summary = self.info_client.fetch_live_account_summary().await?;
        let live_positions = self.info_client.fetch_live_account_positions().await?;

        self.release_reentry_blocks(&signals);
        let positions_changed = self
            .sync_tracked_positions(&live_positions, &snapshot.market_meta, &signals)
            .await?;
        self.reconcile_stale_pending_entries(&live_positions)?;

        self.process_signal_exits(&signals).await?;
        self.process_entries(&snapshot, &signals, live_summary.as_ref())
            .await?;

        if positions_changed
            || self.cycle_count == 1
            || self
                .cycle_count
                .is_multiple_of(self.cfg.sltp_reconcile_every_cycles)
        {
            self.reconcile_sltp_orders().await?;
        }
        self.reconcile_fill_journal().await?;

        self.state_store.save(&self.state)?;
        if self.cycle_count.is_multiple_of(10) {
            self.log_summary("cycle")?;
        }
        info!(
            "cycle complete symbols={} open_positions={} reentry_blocks={}",
            snapshot.market_meta.len(),
            self.state.positions.len(),
            self.state.reentry_blocks.len()
        );
        Ok(())
    }

    fn evaluate_signals(&self, snapshot: &AccountDataSnapshot) -> HashMap<String, CombinedSignal> {
        let mut decisions = HashMap::new();
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
            );

            let dead_component = SignalComponent {
                direction: dead_eval.signal,
                strength: dead_eval.strength,
                reason: dead_eval.reason,
            };
            let whale_component = SignalComponent {
                direction: whale_eval.signal,
                strength: whale_eval.strength,
                reason: whale_eval.reason,
            };
            let (net_score, target_direction, raw_notional_usd, order_notional_usd) =
                combined_target(
                    &dead_component,
                    &whale_component,
                    self.cfg.base_notional_usd,
                    self.cfg.min_trade_notional_usd,
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
                        "net={:+.3} dead={}({:.2}) whale={}({:.2})",
                        net_score,
                        dead_eval.signal,
                        dead_eval.strength,
                        whale_eval.signal,
                        whale_eval.strength
                    ),
                },
            );
        }
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

    async fn sync_tracked_positions(
        &mut self,
        live_positions: &HashMap<String, LivePosition>,
        market_meta: &HashMap<String, MarketMeta>,
        signals: &HashMap<String, CombinedSignal>,
    ) -> Result<bool> {
        let tracked_symbols: Vec<String> = self.state.positions.keys().cloned().collect();
        let mut remove_symbols = Vec::new();
        let mut changed = false;

        for symbol in tracked_symbols {
            let Some(tracked) = self.state.positions.get(&symbol).cloned() else {
                continue;
            };
            match live_positions.get(&symbol) {
                None => {
                    let (exit_price, exit_reason) = classify_disappeared_position(
                        &tracked,
                        market_meta.get(&symbol).map(|meta| meta.mark_px),
                        self.cfg.sl_atr_multiplier,
                        self.cfg.tp_atr_multiplier,
                    );
                    info!(
                        "[{}] live position disappeared; classifying as {} @ {:.6}",
                        symbol, exit_reason, exit_price
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
            let (stop_price, take_profit_price) = self
                .build_exit_levels(
                    &live.symbol,
                    &live.exchange_symbol,
                    live.direction,
                    live.entry_price,
                )
                .await
                .unwrap_or((0.0, 0.0));
            let opened_at_ms =
                estimated_adopted_open_time(crate::now_ms(), self.cfg.min_hold_hours);
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
                    trade_id,
                    entry_source: "adopted".to_string(),
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
        let min_hold_ms = (self.cfg.min_hold_hours * 3600.0 * 1000.0) as u64;
        let symbols: Vec<String> = self.state.positions.keys().cloned().collect();

        for symbol in symbols {
            let Some(position) = self.state.positions.get(&symbol).cloned() else {
                continue;
            };
            if now_ms.saturating_sub(position.opened_at_ms) < min_hold_ms {
                continue;
            }
            let Some(signal) = signals.get(&symbol) else {
                continue;
            };
            let dead_opposes = signal.dead_cap.direction.opposes(position.direction);
            let whale_opposes = signal.whale.direction.opposes(position.direction);
            if !dead_opposes && !whale_opposes {
                continue;
            }

            let mut reasons = Vec::new();
            if dead_opposes {
                reasons.push(format!("dead_cap={}", signal.dead_cap.direction));
            }
            if whale_opposes {
                reasons.push(format!("whale={}", signal.whale.direction));
            }

            match self
                .exit_position(&position, &reasons.join(" "), Some(signal))
                .await?
            {
                ExitDisposition::Closed => {
                    self.state.positions.remove(&symbol);
                    self.state
                        .reentry_blocks
                        .insert(symbol.clone(), position.direction);
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

        let available_slots = self
            .cfg
            .max_open_positions
            .saturating_sub(self.state.positions.len());
        if available_slots == 0 {
            return Ok(());
        }

        let mut candidates = signals
            .values()
            .filter(|signal| signal.target_direction.is_actionable())
            .filter(|signal| !self.state.positions.contains_key(&signal.symbol))
            .filter(|signal| {
                self.state.reentry_blocks.get(&signal.symbol).copied()
                    != Some(signal.target_direction)
            })
            .filter(|signal| self.executor.is_supported_symbol(&signal.symbol))
            .filter_map(|signal| {
                snapshot
                    .market_meta
                    .get(&signal.symbol)
                    .filter(|meta| meta.mark_px > 0.0)
                    .map(|_| signal.clone())
            })
            .collect::<Vec<_>>();

        candidates.sort_by(|left, right| {
            right
                .net_score
                .abs()
                .partial_cmp(&left.net_score.abs())
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.symbol.cmp(&right.symbol))
        });

        for signal in candidates.into_iter().take(available_slots) {
            if self.journal.find_open_trade(&signal.symbol)?.is_some() {
                continue;
            }
            let Some(meta) = snapshot.market_meta.get(&signal.symbol) else {
                continue;
            };
            if meta.mark_px <= 0.0 {
                continue;
            }

            let requested_qty = self
                .executor
                .round_size(signal.order_notional_usd / meta.mark_px, &signal.symbol);
            if requested_qty <= 0.0 {
                continue;
            }
            if self.cfg.dry_run {
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
                EntryAttemptOutcome::Entered | EntryAttemptOutcome::Rejected => {}
                EntryAttemptOutcome::MarginExhausted => break,
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
        let atr = match self
            .atr_client
            .fetch(&self.info_client, &signal.exchange_symbol)
            .await
        {
            Ok(data) if data.atr > 0.0 => data,
            Ok(_) => {
                warn!("[{}] ATR unavailable before entry; skipping", signal.symbol);
                return Ok(EntryAttemptOutcome::Rejected);
            }
            Err(err) => {
                warn!("[{}] ATR fetch failed before entry: {}", signal.symbol, err);
                return Ok(EntryAttemptOutcome::Rejected);
            }
        };
        let trade_id = self.journal.log_entry_intent(
            &signal,
            signal.target_direction,
            meta.mark_px,
            requested_qty,
            "signal",
            Some("pre-order entry intent"),
        )?;

        let result = self
            .executor
            .place_market_order(
                &signal.symbol,
                signal.target_direction,
                requested_qty,
                false,
            )
            .await?;
        let live_after = self.executor.get_position(&signal.symbol).await?;
        let live_matches_signal = live_after
            .as_ref()
            .filter(|live| live.direction == signal.target_direction && live.qty > 0.0);
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

        let (stop_price, take_profit_price, exit_side) = match signal.target_direction {
            Direction::Long => (
                self.executor.round_price(
                    fill_price - (atr.atr * self.cfg.sl_atr_multiplier),
                    &signal.symbol,
                ),
                self.executor.round_price(
                    fill_price + (atr.atr * self.cfg.tp_atr_multiplier),
                    &signal.symbol,
                ),
                Direction::Short,
            ),
            Direction::Short => (
                self.executor.round_price(
                    fill_price + (atr.atr * self.cfg.sl_atr_multiplier),
                    &signal.symbol,
                ),
                self.executor.round_price(
                    fill_price - (atr.atr * self.cfg.tp_atr_multiplier),
                    &signal.symbol,
                ),
                Direction::Long,
            ),
            Direction::Neutral => return Ok(EntryAttemptOutcome::Rejected),
        };

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
                .place_stop_order(
                    &signal.symbol,
                    exit_side,
                    fill_size,
                    take_profit_price,
                    "tp",
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
                trade_id: Some(trade_id),
                entry_source: "signal".to_string(),
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

    async fn exit_position(
        &mut self,
        position: &TrackedPosition,
        reason: &str,
        exit_signal: Option<&CombinedSignal>,
    ) -> Result<ExitDisposition> {
        self.cancel_symbol_sltp_orders(position).await?;
        let close_side = opposite_direction(position.direction);
        let result = self
            .executor
            .place_market_order(&position.symbol, close_side, position.qty, true)
            .await?;
        if !result.success {
            warn!(
                "[{}] exit rejected side={} qty={:.6} err={:?}",
                position.symbol, close_side, position.qty, result.error
            );
            if let Some(trade_id) = position.trade_id.or(self
                .journal
                .find_open_trade(&position.symbol)?
                .map(|trade| trade.id))
            {
                let _ = self.journal.close_trade(
                    trade_id,
                    position.entry_price,
                    "EXIT_FAILED",
                    exit_signal,
                )?;
                self.journal.log_event(
                    Some(trade_id),
                    &position.symbol,
                    "EXIT_FAILED",
                    reason,
                    exit_signal,
                )?;
            }
            return Ok(ExitDisposition::Detached);
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
            let live = self.executor.get_position(symbol).await?;
            match live {
                None => return Ok(true),
                Some(position) if position.is_flat() => return Ok(true),
                Some(position) => {
                    let min_size = self.executor.min_size(symbol);
                    if position.qty < size_epsilon(position.qty) || position.qty < min_size {
                        return Ok(true);
                    }

                    let retry = self
                        .executor
                        .place_market_order(
                            symbol,
                            close_side,
                            position.qty.max(fallback_qty),
                            true,
                        )
                        .await?;
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

    async fn reconcile_sltp_orders(&mut self) -> Result<()> {
        if self.cfg.dry_run {
            return Ok(());
        }

        let positions: Vec<TrackedPosition> = self.state.positions.values().cloned().collect();
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
                    .place_stop_order(
                        &position.symbol,
                        exit_side,
                        position.qty,
                        position.take_profit_price,
                        "tp",
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
    ) -> Result<(f64, f64)> {
        if !direction.is_actionable() || entry_price <= 0.0 {
            return Ok((0.0, 0.0));
        }
        let atr = self
            .atr_client
            .fetch(&self.info_client, exchange_symbol)
            .await?;
        if atr.atr <= 0.0 {
            return Ok((0.0, 0.0));
        }
        let levels = match direction {
            Direction::Long => (
                self.executor
                    .round_price(entry_price - (atr.atr * self.cfg.sl_atr_multiplier), symbol),
                self.executor
                    .round_price(entry_price + (atr.atr * self.cfg.tp_atr_multiplier), symbol),
            ),
            Direction::Short => (
                self.executor
                    .round_price(entry_price + (atr.atr * self.cfg.sl_atr_multiplier), symbol),
                self.executor
                    .round_price(entry_price - (atr.atr * self.cfg.tp_atr_multiplier), symbol),
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

    fn reconcile_stale_pending_entries(
        &self,
        live_positions: &HashMap<String, LivePosition>,
    ) -> Result<()> {
        let cutoff_ms = crate::now_ms().saturating_sub(
            (self.cfg.chase_limit_timeout_secs * 1000) + (self.cfg.loop_interval_secs * 1000),
        ) as i64;
        for pending in self.journal.pending_entries_before(cutoff_ms)? {
            if live_positions.contains_key(&pending.symbol)
                || self.state.positions.contains_key(&pending.symbol)
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

fn live_qty_epsilon(qty: f64) -> f64 {
    qty.abs().max(1.0) * 1e-6
}

fn is_insufficient_margin_error(err: &str) -> bool {
    err.to_ascii_lowercase().contains("insufficient margin")
}

#[cfg(test)]
fn infer_sltp_presence(position: &TrackedPosition, orders: &[OpenOrder]) -> (bool, bool) {
    let classified = classify_sltp_orders(position, orders);
    (
        !classified.sl_order_ids.is_empty(),
        !classified.tp_order_ids.is_empty(),
    )
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
    sl_atr_multiplier: f64,
    tp_atr_multiplier: f64,
) -> (f64, String) {
    let exit_price = mark_price
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
    let mut has_explicit_types = false;
    for order in orders {
        if !order.reduce_only {
            continue;
        }
        match order.order_type.as_str() {
            "sl" => {
                has_explicit_types = true;
                out.sl_qty += order.size;
                out.sl_order_ids.insert(order.order_id.clone());
            }
            "tp" => {
                has_explicit_types = true;
                out.tp_qty += order.size;
                out.tp_order_ids.insert(order.order_id.clone());
            }
            _ => {}
        }
    }
    if has_explicit_types {
        return out;
    }

    for order in orders {
        if !order.reduce_only {
            continue;
        }
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
    out
}

fn size_matches(expected: f64, actual: f64) -> bool {
    (expected - actual).abs() <= live_qty_epsilon(expected.max(actual))
}

pub fn combined_target(
    dead_cap: &SignalComponent,
    whale: &SignalComponent,
    base_notional_usd: f64,
    min_trade_notional_usd: f64,
) -> (f64, Direction, f64, f64) {
    let net_score = dead_cap.signed_strength(0.5) + whale.signed_strength(0.5);
    let target_direction = Direction::from_score(net_score);
    let raw_notional_usd = base_notional_usd * net_score.abs();
    let order_notional_usd = if target_direction.is_actionable() {
        raw_notional_usd.max(min_trade_notional_usd)
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

pub fn should_release_reentry_block(
    blocked_direction: Direction,
    target_direction: Direction,
) -> bool {
    target_direction == Direction::Neutral || target_direction.opposes(blocked_direction)
}

#[cfg(test)]
mod tests {
    use super::infer_sltp_presence;
    use crate::types::{CombinedSignal, Direction, OpenOrder, SignalComponent, TrackedPosition};

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
            trade_id: None,
            entry_source: "signal".to_string(),
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
}
