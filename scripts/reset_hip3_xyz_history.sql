PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

CREATE TEMP TABLE IF NOT EXISTS _reset_xyz_trade_ids (id INTEGER PRIMARY KEY);
DELETE FROM _reset_xyz_trade_ids;
INSERT INTO _reset_xyz_trade_ids(id)
SELECT id
FROM trades
WHERE symbol LIKE 'XYZ:%';

CREATE TEMP TABLE IF NOT EXISTS _reset_xyz_proposal_ids (id INTEGER PRIMARY KEY);
DELETE FROM _reset_xyz_proposal_ids;
INSERT INTO _reset_xyz_proposal_ids(id)
SELECT id
FROM trade_proposals
WHERE symbol LIKE 'XYZ:%';

CREATE TEMP TABLE IF NOT EXISTS _reset_xyz_symbols (symbol TEXT PRIMARY KEY);
DELETE FROM _reset_xyz_symbols;
INSERT OR IGNORE INTO _reset_xyz_symbols(symbol)
SELECT DISTINCT symbol FROM trades WHERE symbol LIKE 'XYZ:%';
INSERT OR IGNORE INTO _reset_xyz_symbols(symbol)
SELECT DISTINCT symbol FROM trade_proposals WHERE symbol LIKE 'XYZ:%';
INSERT OR IGNORE INTO _reset_xyz_symbols(symbol)
SELECT DISTINCT symbol FROM symbol_learning_state WHERE symbol LIKE 'XYZ:%';

-- Proposal dependency cleanup.
DELETE FROM proposal_metadata
WHERE proposal_id IN (SELECT id FROM _reset_xyz_proposal_ids);

DELETE FROM proposal_status_history
WHERE proposal_id IN (SELECT id FROM _reset_xyz_proposal_ids);

DELETE FROM proposal_executions
WHERE proposal_id IN (SELECT id FROM _reset_xyz_proposal_ids);

-- Trade dependency cleanup (trade_id keyed).
DELETE FROM fills
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM position_actions
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids);

DELETE FROM trade_features
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM trade_reflections
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids);

DELETE FROM reflections_v2
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids);

DELETE FROM reflection_tasks_v1
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM decay_flags
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM decay_decisions
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM symbol_learning_processed
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM trade_episodes
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM episode_summaries
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids);

DELETE FROM agi_decisions
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM funding_arb_pair_legs
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids)
   OR symbol IN (SELECT symbol FROM _reset_xyz_symbols);

DELETE FROM rebalance_outcomes
WHERE trade_id IN (SELECT id FROM _reset_xyz_trade_ids);

-- Symbol-level rollups/policy/state cleanup.
DELETE FROM symbol_learning_state
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM signal_symbol_stats
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM combo_symbol_stats
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM symbol_learning_rollups
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM symbol_conclusions_v1
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM symbol_policy
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

-- Ancillary symbol caches/log tables.
DELETE FROM memory_items
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM monitor_positions
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM orphan_orders
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM pending_orders
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

DELETE FROM actor_critic_runs
WHERE symbol IN (SELECT symbol FROM _reset_xyz_symbols)
   OR symbol LIKE 'XYZ:%';

-- Primary rows last.
DELETE FROM trade_proposals
WHERE id IN (SELECT id FROM _reset_xyz_proposal_ids);

DELETE FROM trades
WHERE id IN (SELECT id FROM _reset_xyz_trade_ids);

COMMIT;
PRAGMA foreign_keys = ON;
