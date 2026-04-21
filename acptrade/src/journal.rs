use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::types::{CombinedSignal, DeadCapSnapshot, Direction, UserFill};

#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub id: i64,
    pub symbol: String,
    pub direction: Direction,
    pub entry_time_ms: i64,
    pub entry_price: f64,
    pub size: f64,
    pub status: String,
    pub acp_job_id: Option<String>,
    pub acp_phase: Option<String>,
    pub hl_order_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct JournalSummary {
    pub total_trades: u64,
    pub open_trades: u64,
    pub closed_trades: u64,
    pub total_pnl: f64,
}

#[derive(Debug, Clone)]
struct FillMatchTrade {
    id: i64,
    status: String,
    exit_reason: Option<String>,
}

pub struct TradeJournal {
    path: PathBuf,
}

impl TradeJournal {
    pub fn new(path: PathBuf) -> Result<Self> {
        let journal = Self { path };
        journal.init_db()?;
        Ok(journal)
    }

    pub fn log_entry_intent(
        &self,
        signal: &CombinedSignal,
        direction: Direction,
        intended_price: f64,
        intended_size: f64,
        entry_source: &str,
        notes: Option<&str>,
    ) -> Result<i64> {
        let now_ms = crate::now_ms() as i64;
        let snapshot = serde_json::to_string(signal)?;
        let conn = self.connection()?;
        conn.execute(
            "INSERT INTO trades (
                symbol, direction, entry_time_ms, entry_price, size, notional_usd, status,
                entry_source, entry_snapshot, entry_intent_price, entry_intent_size, notes,
                created_at_ms, updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'PENDING_ENTRY', ?7, ?8, ?9, ?10, ?11, ?12, ?12)",
            params![
                signal.symbol,
                direction.as_str(),
                now_ms,
                intended_price,
                intended_size,
                intended_price * intended_size,
                entry_source,
                snapshot,
                intended_price,
                intended_size,
                notes,
                now_ms,
            ],
        )
        .context("failed to insert trade entry intent")?;
        Ok(conn.last_insert_rowid())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn log_adopted_trade(
        &self,
        symbol: &str,
        direction: Direction,
        entry_price: f64,
        size: f64,
        entry_signal: &CombinedSignal,
        entry_source: &str,
        notes: Option<&str>,
    ) -> Result<i64> {
        let now_ms = crate::now_ms() as i64;
        let snapshot = serde_json::to_string(entry_signal)?;
        let conn = self.connection()?;
        conn.execute(
            "INSERT INTO trades (
                symbol, direction, entry_time_ms, entry_price, size, notional_usd, status,
                entry_source, entry_snapshot, notes, created_at_ms, updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'OPEN', ?7, ?8, ?9, ?10, ?10)",
            params![
                symbol,
                direction.as_str(),
                now_ms,
                entry_price,
                size,
                entry_price * size,
                entry_source,
                snapshot,
                notes,
                now_ms,
            ],
        )
        .context("failed to insert adopted trade")?;
        Ok(conn.last_insert_rowid())
    }

    pub fn confirm_entry_fill(&self, trade_id: i64, fill_price: f64, fill_size: f64) -> Result<()> {
        let now_ms = crate::now_ms() as i64;
        self.connection()?.execute(
            "UPDATE trades
             SET entry_price = ?2,
                 size = ?3,
                 notional_usd = ?2 * ?3,
                 status = 'OPEN',
                 acp_phase = CASE WHEN acp_job_id IS NULL THEN acp_phase ELSE 'FILLED' END,
                 updated_at_ms = ?4
             WHERE id = ?1",
            params![trade_id, fill_price, fill_size, now_ms],
        )?;
        Ok(())
    }

    pub fn attach_acp_job(&self, trade_id: i64, job_id: &str, phase: Option<&str>) -> Result<()> {
        let now_ms = crate::now_ms() as i64;
        self.connection()?.execute(
            "UPDATE trades
             SET acp_job_id = ?2,
                 acp_phase = ?3,
                 updated_at_ms = ?4
             WHERE id = ?1",
            params![trade_id, job_id, phase, now_ms],
        )?;
        Ok(())
    }

    pub fn update_acp_phase(&self, trade_id: i64, phase: Option<&str>) -> Result<()> {
        let now_ms = crate::now_ms() as i64;
        self.connection()?.execute(
            "UPDATE trades
             SET acp_phase = ?2,
                 updated_at_ms = ?3
             WHERE id = ?1",
            params![trade_id, phase, now_ms],
        )?;
        Ok(())
    }

    pub fn attach_hl_order_id(&self, trade_id: i64, order_id: &str) -> Result<()> {
        let now_ms = crate::now_ms() as i64;
        self.connection()?.execute(
            "UPDATE trades
             SET hl_order_id = ?2,
                 updated_at_ms = ?3
             WHERE id = ?1",
            params![trade_id, order_id, now_ms],
        )?;
        Ok(())
    }

    pub fn mark_entry_failed(&self, trade_id: i64, reason: &str) -> Result<()> {
        let now_ms = crate::now_ms() as i64;
        self.connection()?.execute(
            "UPDATE trades
             SET status = 'CLOSED',
                 exit_time_ms = ?2,
                 exit_reason = ?3,
                 realized_pnl = 0.0,
                 updated_at_ms = ?2
             WHERE id = ?1",
            params![trade_id, now_ms, reason],
        )?;
        Ok(())
    }

    pub fn close_trade(
        &self,
        trade_id: i64,
        exit_price: f64,
        exit_reason: &str,
        exit_signal: Option<&CombinedSignal>,
    ) -> Result<Option<f64>> {
        let conn = self.connection()?;
        let trade = self.get_trade_by_id_with_conn(&conn, trade_id)?;
        let Some(trade) = trade else {
            return Ok(None);
        };
        let pnl = realized_pnl(trade.direction, trade.entry_price, exit_price, trade.size);
        let exit_snapshot = match exit_signal {
            Some(signal) => Some(serde_json::to_string(signal)?),
            None => None,
        };
        let now_ms = crate::now_ms() as i64;
        conn.execute(
            "UPDATE trades
             SET status = 'CLOSED',
                 exit_time_ms = ?2,
                 exit_price = ?3,
                 exit_reason = ?4,
                 realized_pnl = ?5,
                 exit_snapshot = ?6,
                 updated_at_ms = ?2
             WHERE id = ?1",
            params![
                trade_id,
                now_ms,
                exit_price,
                exit_reason,
                pnl,
                exit_snapshot
            ],
        )?;
        Ok(Some(pnl))
    }

    pub fn find_open_trade(&self, symbol: &str) -> Result<Option<TradeRecord>> {
        self.connection()?
            .query_row(
            "SELECT id, symbol, direction, entry_time_ms, entry_price, size, status, acp_job_id, acp_phase
                    , hl_order_id
             FROM trades
             WHERE symbol = ?1 AND status IN ('OPEN', 'PENDING_ENTRY')
                 ORDER BY id DESC
                 LIMIT 1",
                params![symbol],
                trade_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn open_trade_symbols(&self) -> Result<HashSet<String>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT symbol
             FROM trades
             WHERE status IN ('OPEN', 'PENDING_ENTRY')",
        )?;
        let rows = stmt.query_map(params![], |row| row.get::<_, String>(0))?;
        let mut out = HashSet::new();
        for row in rows {
            out.insert(row?);
        }
        Ok(out)
    }

    pub fn log_event<T: Serialize>(
        &self,
        trade_id: Option<i64>,
        symbol: &str,
        kind: &str,
        message: &str,
        payload: Option<&T>,
    ) -> Result<()> {
        let payload_json = match payload {
            Some(value) => Some(serde_json::to_string(value)?),
            None => None,
        };
        let now_ms = crate::now_ms() as i64;
        self.connection()?.execute(
            "INSERT INTO trade_events (trade_id, symbol, kind, message, payload_json, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![trade_id, symbol, kind, message, payload_json, now_ms],
        )?;
        Ok(())
    }

    pub fn pending_entries_before(&self, cutoff_ms: i64) -> Result<Vec<TradeRecord>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT id, symbol, direction, entry_time_ms, entry_price, size, status, acp_job_id, acp_phase
                    , hl_order_id
             FROM trades
             WHERE status = 'PENDING_ENTRY' AND entry_time_ms <= ?1
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![cutoff_ms], trade_row)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn pending_entries(&self) -> Result<Vec<TradeRecord>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT id, symbol, direction, entry_time_ms, entry_price, size, status, acp_job_id, acp_phase
                    , hl_order_id
             FROM trades
             WHERE status = 'PENDING_ENTRY'
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![], trade_row)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn summary(&self) -> Result<JournalSummary> {
        let conn = self.connection()?;
        let total_trades = scalar_u64(&conn, "SELECT COUNT(*) FROM trades", params![])?;
        let open_trades = scalar_u64(
            &conn,
            "SELECT COUNT(*) FROM trades WHERE status IN ('OPEN', 'PENDING_ENTRY')",
            params![],
        )?;
        let total_pnl = conn.query_row(
            "SELECT COALESCE(SUM(realized_pnl), 0.0) FROM trades WHERE status = 'CLOSED'",
            params![],
            |row| row.get::<_, f64>(0),
        )?;
        Ok(JournalSummary {
            total_trades,
            open_trades,
            closed_trades: total_trades.saturating_sub(open_trades),
            total_pnl,
        })
    }

    pub fn symbol_loss_streaks(&self, reset_after_losses: u32) -> Result<HashMap<String, u32>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT symbol, COALESCE(realized_pnl, 0.0)
             FROM trades
             WHERE status = 'CLOSED'
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
        })?;

        let mut streaks = HashMap::new();
        for row in rows {
            let (symbol, realized_pnl) = row?;
            let current = streaks.get(&symbol).copied().unwrap_or(0);
            let next = next_loss_streak(current, realized_pnl, reset_after_losses);
            if next == 0 {
                streaks.remove(&symbol);
            } else {
                streaks.insert(symbol, next);
            }
        }
        Ok(streaks)
    }

    pub fn last_fill_time_ms(&self) -> Result<Option<i64>> {
        self.connection()?
            .query_row("SELECT MAX(fill_time_ms) FROM fills", params![], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .map_err(Into::into)
    }

    pub fn find_trade_for_fill(&self, symbol: &str, fill_time_ms: i64) -> Result<Option<i64>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT id, status, exit_reason
             FROM trades
             WHERE symbol = ?1
               AND entry_time_ms <= ?2
             ORDER BY id DESC",
        )?;
        let mut rows = stmt.query(params![symbol, fill_time_ms])?;
        while let Some(row) = rows.next()? {
            let trade = FillMatchTrade {
                id: row.get(0)?,
                status: row.get(1)?,
                exit_reason: row.get(2)?,
            };
            if trade.status == "PENDING_ENTRY" || trade.status == "OPEN" {
                return Ok(Some(trade.id));
            }
            if trade.status == "CLOSED" {
                let exit_like = trade.exit_reason.as_deref().unwrap_or_default();
                if fill_matches_closed_trade_exit_reason(exit_like) {
                    return Ok(Some(trade.id));
                }
            }
        }
        Ok(None)
    }

    pub fn log_fill(
        &self,
        trade_id: Option<i64>,
        fill: &UserFill,
        fill_type: &str,
    ) -> Result<bool> {
        let raw_json = serde_json::to_string(fill)?;
        let inserted = self.connection()?.execute(
            "INSERT OR IGNORE INTO fills (
                trade_id, external_fill_id, symbol, side, fill_time_ms, fill_price, fill_size,
                fill_type, oid, tid, fee_usd, builder_fee_usd, raw_json, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                trade_id,
                fill.fill_id,
                fill.symbol,
                fill.side.as_str(),
                fill.ts_ms as i64,
                fill.price,
                fill.size,
                fill_type,
                fill.oid.map(|value| value as i64),
                fill.tid.map(|value| value as i64),
                fill.fee_usd,
                fill.builder_fee_usd,
                raw_json,
                crate::now_ms() as i64,
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn log_dead_cap_history(
        &self,
        snapshots: impl IntoIterator<Item = DeadCapSnapshot>,
    ) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction()?;
        let now_ms = crate::now_ms() as i64;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO dead_cap_history (
                    created_at_ms, symbol, signal, strength, threshold,
                    locked_long_pct, locked_short_pct,
                    effective_long_pct, effective_short_pct,
                    bad_long_pct, bad_short_pct,
                    smart_long_pct, smart_short_pct,
                    observed_pct, locked_wallet_count,
                    dominant_top_share, persistence_streak, reason
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            )?;
            for snapshot in snapshots {
                stmt.execute(params![
                    now_ms,
                    snapshot.symbol,
                    snapshot.signal.as_str(),
                    snapshot.strength,
                    snapshot.threshold,
                    snapshot.locked_long_pct,
                    snapshot.locked_short_pct,
                    snapshot.effective_long_pct,
                    snapshot.effective_short_pct,
                    snapshot.bad_long_pct,
                    snapshot.bad_short_pct,
                    snapshot.smart_long_pct,
                    snapshot.smart_short_pct,
                    snapshot.observed_pct,
                    snapshot.locked_wallet_count as i64,
                    snapshot.dominant_top_share,
                    snapshot.persistence_streak as i64,
                    snapshot.reason,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn init_db(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create journal dir {}", parent.display()))?;
        }
        let conn = self.connection()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_time_ms INTEGER NOT NULL,
                entry_price REAL NOT NULL,
                size REAL NOT NULL,
                notional_usd REAL NOT NULL,
                status TEXT NOT NULL,
                entry_source TEXT NOT NULL,
                entry_snapshot TEXT,
                entry_intent_price REAL,
                entry_intent_size REAL,
                exit_time_ms INTEGER,
                exit_price REAL,
                exit_reason TEXT,
                realized_pnl REAL,
                exit_snapshot TEXT,
                notes TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_symbol_status
                ON trades (symbol, status);
            CREATE TABLE IF NOT EXISTS trade_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER,
                symbol TEXT NOT NULL,
                kind TEXT NOT NULL,
                message TEXT NOT NULL,
                payload_json TEXT,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trade_events_symbol
                ON trade_events (symbol, created_at_ms);
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER,
                external_fill_id TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                fill_time_ms INTEGER NOT NULL,
                fill_price REAL NOT NULL,
                fill_size REAL NOT NULL,
                fill_type TEXT NOT NULL,
                oid INTEGER,
                tid INTEGER,
                fee_usd REAL,
                builder_fee_usd REAL,
                raw_json TEXT,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_fills_symbol_time
                ON fills (symbol, fill_time_ms);
            CREATE INDEX IF NOT EXISTS idx_fills_trade_id
                ON fills (trade_id, fill_time_ms);
            CREATE TABLE IF NOT EXISTS dead_cap_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                signal TEXT NOT NULL,
                strength REAL NOT NULL,
                threshold REAL NOT NULL,
                locked_long_pct REAL NOT NULL,
                locked_short_pct REAL NOT NULL,
                effective_long_pct REAL NOT NULL,
                effective_short_pct REAL NOT NULL,
                bad_long_pct REAL NOT NULL,
                bad_short_pct REAL NOT NULL,
                smart_long_pct REAL NOT NULL,
                smart_short_pct REAL NOT NULL,
                observed_pct REAL NOT NULL,
                locked_wallet_count INTEGER NOT NULL,
                dominant_top_share REAL NOT NULL,
                persistence_streak INTEGER NOT NULL,
                reason TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dead_cap_history_symbol_time
                ON dead_cap_history (symbol, created_at_ms);
            CREATE INDEX IF NOT EXISTS idx_dead_cap_history_time
                ON dead_cap_history (created_at_ms);",
        )?;
        ensure_trade_column(&conn, "acp_job_id", "TEXT")?;
        ensure_trade_column(&conn, "acp_phase", "TEXT")?;
        ensure_trade_column(&conn, "hl_order_id", "TEXT")?;
        Ok(())
    }

    fn connection(&self) -> Result<Connection> {
        Connection::open(&self.path)
            .with_context(|| format!("failed to open journal {}", self.path.display()))
    }

    fn get_trade_by_id_with_conn(
        &self,
        conn: &Connection,
        trade_id: i64,
    ) -> Result<Option<TradeRecord>> {
        conn.query_row(
            "SELECT id, symbol, direction, entry_time_ms, entry_price, size, status, acp_job_id, acp_phase,
                    hl_order_id
             FROM trades
             WHERE id = ?1",
            params![trade_id],
            trade_row,
        )
        .optional()
        .map_err(Into::into)
    }
}

fn trade_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TradeRecord> {
    Ok(TradeRecord {
        id: row.get(0)?,
        symbol: row.get(1)?,
        direction: Direction::from_db_str(&row.get::<_, String>(2)?),
        entry_time_ms: row.get(3)?,
        entry_price: row.get(4)?,
        size: row.get(5)?,
        status: row.get(6)?,
        acp_job_id: row.get(7)?,
        acp_phase: row.get(8)?,
        hl_order_id: row.get(9)?,
    })
}

fn ensure_trade_column(conn: &Connection, name: &str, definition: &str) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(trades)")?;
    let mut rows = stmt.query(params![])?;
    while let Some(row) = rows.next()? {
        let existing: String = row.get(1)?;
        if existing == name {
            return Ok(());
        }
    }
    conn.execute(
        &format!("ALTER TABLE trades ADD COLUMN {} {}", name, definition),
        params![],
    )?;
    Ok(())
}

fn scalar_u64<P>(conn: &Connection, sql: &str, params: P) -> Result<u64>
where
    P: rusqlite::Params,
{
    conn.query_row(sql, params, |row| row.get::<_, u64>(0))
        .map_err(Into::into)
}

fn realized_pnl(direction: Direction, entry_price: f64, exit_price: f64, size: f64) -> f64 {
    match direction {
        Direction::Long => (exit_price - entry_price) * size,
        Direction::Short => (entry_price - exit_price) * size,
        Direction::Neutral => 0.0,
    }
}

fn fill_matches_closed_trade_exit_reason(exit_reason: &str) -> bool {
    matches!(
        exit_reason,
        "EXIT" | "SL" | "TP" | "LIKELY_SL" | "LIKELY_TP" | "EXTERNAL_CLOSE"
    ) || exit_reason.starts_with("PRESSURE_DECAY")
}

fn next_loss_streak(current: u32, realized_pnl: f64, reset_after_losses: u32) -> u32 {
    if realized_pnl > 0.0 {
        return 0;
    }
    if realized_pnl < 0.0 {
        let next = current.saturating_add(1);
        return if reset_after_losses > 0 && next >= reset_after_losses {
            0
        } else {
            next
        };
    }
    current
}

#[cfg(test)]
mod tests {
    use super::fill_matches_closed_trade_exit_reason;

    #[test]
    fn pressure_decay_closed_trades_can_absorb_late_fills() {
        assert!(fill_matches_closed_trade_exit_reason(
            "PRESSURE_DECAY 9.25% < 9.60% (80% of 12.00%)"
        ));
    }

    #[test]
    fn order_failed_closed_trades_do_not_absorb_late_fills() {
        assert!(!fill_matches_closed_trade_exit_reason("ORDER_FAILED"));
    }
}
