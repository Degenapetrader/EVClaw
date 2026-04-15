#!/usr/bin/env node
'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const {
  default: DefaultAcpClient,
  AcpClient,
  ACPClient,
  AcpContractClientV2,
  AcpJobPhases,
  FareAmount,
  baseAcpConfig,
  baseAcpConfigV2,
  baseSepoliaAcpConfigV2,
} = require('@virtuals-protocol/acp-node');

const AcpClientClass = DefaultAcpClient || AcpClient || ACPClient;

if (typeof AcpClientClass !== 'function') {
  console.error('Unable to load AcpClient from @virtuals-protocol/acp-node');
  process.exit(1);
}

const DGCLAW_PROVIDER_ADDRESS = '0xd478a8B40372db16cA8045F28C6FE07228F3781A';
const ENV_PATH = path.resolve(__dirname, '../.env');
const SUPPORTED_OFFERINGS = new Set(['perp_trade', 'perp_modify', 'perp_withdraw', 'perp_deposit']);
const SUPPORTED_COMMANDS = new Set([
  'join',
  'job_status',
  'job_resume',
  'active_jobs',
  ...SUPPORTED_OFFERINGS,
]);
const JOIN_POLL_INTERVAL_MS = 10_000;
const JOIN_TIMEOUT_MS = 10 * 60 * 1000;

function usage() {
  return `Usage:
  node scripts/dgclaw_acp_job.js join
  node scripts/dgclaw_acp_job.js perp_trade --action open --pair BTC --side long --size 10 --leverage 5 --stopLoss 93000 --takeProfit 98000
  node scripts/dgclaw_acp_job.js perp_trade --action close --pair BTC
  node scripts/dgclaw_acp_job.js perp_trade --action cancel_limit --pair BTC --oid 123456789
  node scripts/dgclaw_acp_job.js perp_modify --pair BTC --stopLoss 93000 --takeProfit 98000
  node scripts/dgclaw_acp_job.js perp_deposit --amount 100
  node scripts/dgclaw_acp_job.js perp_withdraw --amount 25 --recipient 0x...
  node scripts/dgclaw_acp_job.js job_status --jobId 123
  node scripts/dgclaw_acp_job.js job_resume --jobId 123 --submit-only
  node scripts/dgclaw_acp_job.js active_jobs
  node scripts/dgclaw_acp_job.js <offering> ... --dry-run

Required env:
  ACP_CLIENT_PRIVATE_KEY
  ACP_CLIENT_WALLET_ADDRESS
  ACP_SESSION_ENTITY_KEY_ID

Optional env:
  ACP_CONTRACT_MODE=v2|v1|base-sepolia   (default: v2)
  ACP_CUSTOM_RPC_URL

  Supported offerings:
  join          Creates join_leaderboard job, auto-pays if needed, decrypts encryptedApiKey, saves DGCLAW_API_KEY into .env
  job_status    Fetches a single ACP job by id
  job_resume    Re-attaches to an existing ACP job id and continues polling / payment handling
  active_jobs   Lists current active ACP jobs for the configured client
  perp_trade    { action: "open" | "close" | "cancel_limit", pair: string, side?: "long" | "short", size?: string, leverage?: number, orderType?: "market" | "limit", limitPrice?: string, stopLoss?: string, takeProfit?: string, oid?: string, postOnly?: boolean, expireAfterSeconds?: integer }
  perp_modify   { pair: string, leverage?: number, stopLoss?: string, takeProfit?: string }
  perp_withdraw { amount: string, recipient?: string }
  perp_deposit  { amount: string }

Flags:
  --submit-only   Create the job, auto-pay if needed, then return once it has left NEGOTIATION.
`;
}

function parseEnvLine(line) {
  const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
  if (!match) return null;
  let value = match[2];
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    value = value.slice(1, -1);
  }
  return { key: match[1], value };
}

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  const raw = fs.readFileSync(filePath, 'utf8');
  for (const line of raw.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const parsed = parseEnvLine(trimmed);
    if (parsed && !(parsed.key in process.env)) {
      process.env[parsed.key] = parsed.value;
    }
  }
}

function readArgs(argv) {
  if (!argv.length || argv.includes('--help') || argv.includes('-h')) {
    console.log(usage());
    process.exit(0);
  }

  const command = argv.shift();
  if (!SUPPORTED_COMMANDS.has(command)) {
    throw new Error(`Unsupported jobOfferingName: ${command}`);
  }

  const options = {};
  let dryRun = false;
  let submitOnly = false;
  for (let i = 0; i < argv.length; i++) {
    const item = argv[i];
    if (item === '--dry-run') {
      dryRun = true;
      continue;
    }
    if (item === '--submit-only') {
      submitOnly = true;
      continue;
    }
    if (!item.startsWith('--')) {
      throw new Error(`Unknown arg: ${item}`);
    }
    const eq = item.indexOf('=');
    let key;
    let value;
    if (eq === -1) {
      key = item.slice(2);
      if (i + 1 >= argv.length || argv[i + 1].startsWith('--')) {
        throw new Error(`Expected value for ${item}`);
      }
      value = argv[++i];
    } else {
      key = item.slice(2, eq);
      value = item.slice(eq + 1);
    }
    options[key] = value;
  }

  return { command, options, dryRun, submitOnly };
}

function assertNoUnknownKeys(allowed, actual, offering) {
  const unknown = Object.keys(actual).filter((key) => !allowed.has(key));
  if (unknown.length) {
    throw new Error(`${offering} received unsupported keys: ${unknown.join(', ')}`);
  }
}

function validateNumericString(name, value) {
  if (typeof value !== 'string' || value.trim() === '' || Number.isNaN(Number(value))) {
    throw new Error(`${name} must be a numeric string`);
  }
  return value;
}

function parseBooleanFlag(name, value) {
  if (typeof value !== 'string') {
    throw new Error(`${name} must be a boolean string`);
  }
  const normalized = value.trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(normalized)) {
    return false;
  }
  throw new Error(`${name} must be true or false`);
}

function buildRequirement(offering, raw) {
  if (offering === 'join') {
    if (Object.keys(raw).length) {
      throw new Error('join does not accept extra arguments');
    }
    return {};
  }

  if (offering === 'perp_trade') {
    const allowed = new Set([
      'action',
      'pair',
      'side',
      'size',
      'leverage',
      'orderType',
      'limitPrice',
      'stopLoss',
      'takeProfit',
      'oid',
      'postOnly',
      'expireAfterSeconds',
    ]);
    assertNoUnknownKeys(allowed, raw, offering);
    if (!raw.action || !['open', 'close', 'cancel_limit'].includes(raw.action)) {
      throw new Error('perp_trade.action must be "open", "close", or "cancel_limit"');
    }
    if (!raw.pair || typeof raw.pair !== 'string') {
      throw new Error('perp_trade.pair is required and must be a string');
    }
    if (raw.action === 'open' && raw.side === undefined) {
      throw new Error('perp_trade.side is required when action=open');
    }
    if (raw.side !== undefined && !['long', 'short'].includes(raw.side)) {
      throw new Error('perp_trade.side must be "long" or "short"');
    }
    if (raw.size !== undefined) {
      validateNumericString('perp_trade.size', raw.size);
    }
    if (raw.action === 'open' && raw.size === undefined) {
      throw new Error('perp_trade.size is required when action=open');
    }
    if (raw.leverage !== undefined) {
      const parsed = Number(raw.leverage);
      if (!Number.isFinite(parsed)) {
        throw new Error('perp_trade.leverage must be a number');
      }
      raw.leverage = parsed;
    }
    if (raw.orderType !== undefined && !['market', 'limit'].includes(raw.orderType)) {
      throw new Error('perp_trade.orderType must be "market" or "limit"');
    }
    if (raw.limitPrice !== undefined) {
      validateNumericString('perp_trade.limitPrice', raw.limitPrice);
    }
    if (raw.orderType === 'limit' && raw.limitPrice === undefined) {
      throw new Error('perp_trade.limitPrice is required when orderType=limit');
    }
    if (raw.stopLoss !== undefined) {
      validateNumericString('perp_trade.stopLoss', raw.stopLoss);
    }
    if (raw.takeProfit !== undefined) {
      validateNumericString('perp_trade.takeProfit', raw.takeProfit);
    }
    if (raw.oid !== undefined) {
      validateNumericString('perp_trade.oid', raw.oid);
    }
    if (raw.postOnly !== undefined) {
      raw.postOnly = parseBooleanFlag('perp_trade.postOnly', raw.postOnly);
    }
    if (raw.expireAfterSeconds !== undefined) {
      const parsed = Number(raw.expireAfterSeconds);
      if (!Number.isInteger(parsed) || parsed <= 0) {
        throw new Error('perp_trade.expireAfterSeconds must be a positive integer');
      }
      raw.expireAfterSeconds = parsed;
    }
    if (raw.action === 'cancel_limit' && raw.oid === undefined) {
      throw new Error('perp_trade.oid is required when action=cancel_limit');
    }
    return {
      action: raw.action,
      pair: raw.pair,
      ...(raw.side !== undefined ? { side: raw.side } : {}),
      ...(raw.size !== undefined ? { size: raw.size } : {}),
      ...(raw.leverage !== undefined ? { leverage: raw.leverage } : {}),
      ...(raw.orderType !== undefined ? { orderType: raw.orderType } : {}),
      ...(raw.limitPrice !== undefined ? { limitPrice: raw.limitPrice } : {}),
      ...(raw.stopLoss !== undefined ? { stopLoss: raw.stopLoss } : {}),
      ...(raw.takeProfit !== undefined ? { takeProfit: raw.takeProfit } : {}),
      ...(raw.oid !== undefined ? { oid: raw.oid } : {}),
      ...(raw.postOnly !== undefined ? { postOnly: raw.postOnly } : {}),
      ...(raw.expireAfterSeconds !== undefined ? { expireAfterSeconds: raw.expireAfterSeconds } : {}),
    };
  }

  if (offering === 'perp_modify') {
    const allowed = new Set(['pair', 'leverage', 'stopLoss', 'takeProfit']);
    assertNoUnknownKeys(allowed, raw, offering);
    if (!raw.pair || typeof raw.pair !== 'string') {
      throw new Error('perp_modify.pair is required and must be a string');
    }
    if (raw.leverage !== undefined) {
      const parsed = Number(raw.leverage);
      if (!Number.isFinite(parsed)) {
        throw new Error('perp_modify.leverage must be a number');
      }
      raw.leverage = parsed;
    }
    if (raw.stopLoss !== undefined) {
      validateNumericString('perp_modify.stopLoss', raw.stopLoss);
    }
    if (raw.takeProfit !== undefined) {
      validateNumericString('perp_modify.takeProfit', raw.takeProfit);
    }
    if (raw.leverage === undefined && raw.stopLoss === undefined && raw.takeProfit === undefined) {
      throw new Error('perp_modify requires at least one of leverage, stopLoss, or takeProfit');
    }
    return {
      pair: raw.pair,
      ...(raw.leverage !== undefined ? { leverage: raw.leverage } : {}),
      ...(raw.stopLoss !== undefined ? { stopLoss: raw.stopLoss } : {}),
      ...(raw.takeProfit !== undefined ? { takeProfit: raw.takeProfit } : {}),
    };
  }

  if (offering === 'perp_withdraw') {
    const allowed = new Set(['amount', 'recipient']);
    assertNoUnknownKeys(allowed, raw, offering);
    if (!raw.amount) {
      throw new Error('perp_withdraw.amount is required');
    }
    validateNumericString('perp_withdraw.amount', raw.amount);
    if (raw.recipient !== undefined && !/^0x[a-fA-F0-9]{40}$/.test(raw.recipient)) {
      throw new Error('perp_withdraw.recipient must be an Ethereum address');
    }
    return {
      amount: raw.amount,
      ...(raw.recipient !== undefined ? { recipient: raw.recipient } : {}),
    };
  }

  const allowed = new Set(['amount']);
  assertNoUnknownKeys(allowed, raw, offering);
  if (!raw.amount) {
    throw new Error('perp_deposit.amount is required');
  }
  validateNumericString('perp_deposit.amount', raw.amount);
  return { amount: raw.amount };
}

function selectContractConfig(mode) {
  switch ((mode || 'v2').toLowerCase()) {
    case 'v1':
      return baseAcpConfig;
    case 'base-sepolia':
      return baseSepoliaAcpConfigV2;
    case 'v2':
    default:
      return baseAcpConfigV2;
  }
}

function getEnvConfig() {
  const privateKey = process.env.ACP_CLIENT_PRIVATE_KEY
    ? (process.env.ACP_CLIENT_PRIVATE_KEY.startsWith('0x')
        ? process.env.ACP_CLIENT_PRIVATE_KEY
        : `0x${process.env.ACP_CLIENT_PRIVATE_KEY}`)
    : '';
  const config = {
    privateKey,
    walletAddress: process.env.ACP_CLIENT_WALLET_ADDRESS,
    sessionEntityKeyId: process.env.ACP_SESSION_ENTITY_KEY_ID,
    customRpcUrl: process.env.ACP_CUSTOM_RPC_URL,
    contractMode: process.env.ACP_CONTRACT_MODE || 'v2',
  };
  const missing = Object.entries(config)
    .filter(([key, value]) => ['privateKey', 'walletAddress', 'sessionEntityKeyId'].includes(key) && !value)
    .map(([key]) => key);
  if (missing.length) {
    throw new Error(
      `Missing required ACP env: ${missing
        .map((key) => ({
          privateKey: 'ACP_CLIENT_PRIVATE_KEY',
          walletAddress: 'ACP_CLIENT_WALLET_ADDRESS',
          sessionEntityKeyId: 'ACP_SESSION_ENTITY_KEY_ID',
        }[key]))
        .join(', ')}`
    );
  }
  const sessionEntityKeyId = Number(config.sessionEntityKeyId);
  if (!Number.isInteger(sessionEntityKeyId) || sessionEntityKeyId <= 0) {
    throw new Error('ACP_SESSION_ENTITY_KEY_ID must be a positive integer');
  }
  return {
    ...config,
    sessionEntityKeyId,
  };
}

function buildJobPayload(jobOfferingName, requirement) {
  return {
    name: jobOfferingName,
    requirement,
  };
}

function summarizeCompletedJob(job, deliverable) {
  return {
    jobId: job.id,
    phase: phaseName(job.phase),
    rejectionReason: job.rejectionReason || null,
    requirement: job.requirement || null,
    paymentRequestData: job.paymentRequestData || null,
    deliverable: deliverable ?? null,
  };
}

function generateRsaJoinKeyPair() {
  const { publicKey, privateKey } = crypto.generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: { type: 'spki', format: 'pem' },
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
  });
  const oneLinePublicKey = publicKey
    .split('\n')
    .filter((line) => line && !line.startsWith('---'))
    .join('');
  return {
    publicKeyPem: publicKey,
    publicKeyOneLine: oneLinePublicKey,
    privateKeyPem: privateKey,
  };
}

function normalizeDeliverable(deliverable) {
  if (deliverable === undefined || deliverable === null || deliverable === '') {
    return null;
  }
  if (typeof deliverable === 'string') {
    try {
      return JSON.parse(deliverable);
    } catch {
      return { value: deliverable };
    }
  }
  return deliverable;
}

async function resolveDeliverable(acpClient, deliverable) {
  if (typeof deliverable === 'string' && deliverable.startsWith('http')) {
    const accessToken = acpClient.accessToken || (acpClient.getAccessToken ? await acpClient.getAccessToken() : null);
    if (!accessToken) {
      throw new Error('Unable to fetch memo content: ACP access token unavailable');
    }
    const response = await fetch(deliverable, {
      headers: {
        authorization: `Bearer ${accessToken}`,
      },
    });
    if (!response.ok) {
      throw new Error(`Failed to fetch memo content: ${response.status} ${response.statusText}`);
    }
    const payload = await response.json();
    return normalizeDeliverable(payload?.data?.content ?? payload?.data ?? null);
  }
  const normalized = normalizeDeliverable(deliverable);
  if (typeof normalized === 'string' && normalized.startsWith('http')) {
    return resolveDeliverable(acpClient, normalized);
  }
  return normalized;
}

function decryptEncryptedApiKey(encryptedApiKey, privateKeyPem) {
  const ciphertext = Buffer.from(encryptedApiKey, 'base64');
  return crypto.privateDecrypt(
    {
      key: privateKeyPem,
      padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
      oaepHash: 'sha256',
    },
    ciphertext,
  ).toString('utf8');
}

function upsertEnvValue(filePath, key, value) {
  const line = `${key}=${value}`;
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, `${line}\n`, 'utf8');
    return;
  }
  const lines = fs.readFileSync(filePath, 'utf8').split('\n');
  let replaced = false;
  const next = lines.map((existing) => {
    if (existing.startsWith(`${key}=`)) {
      replaced = true;
      return line;
    }
    return existing;
  });
  if (!replaced) {
    next.push(line);
  }
  fs.writeFileSync(filePath, `${next.filter((entry, idx, arr) => !(idx === arr.length - 1 && entry === '')).join('\n')}\n`, 'utf8');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function phaseName(phase) {
  const names = {
    [AcpJobPhases.REQUEST]: 'REQUEST',
    [AcpJobPhases.NEGOTIATION]: 'NEGOTIATION',
    [AcpJobPhases.TRANSACTION]: 'TRANSACTION',
    [AcpJobPhases.EVALUATION]: 'EVALUATION',
    [AcpJobPhases.COMPLETED]: 'COMPLETED',
    [AcpJobPhases.REJECTED]: 'REJECTED',
    [AcpJobPhases.EXPIRED]: 'EXPIRED',
  };
  return names[phase] || String(phase);
}

function normalizeJob(job) {
  return {
    jobId: job.id?.toString?.() ?? job.id,
    phase: phaseName(job.phase),
    name: job.name || job.jobOfferingName || null,
    rejectionReason: job.rejectionReason || null,
    requirement: job.requirement || null,
    paymentRequestData: job.paymentRequestData || null,
    deliverable: typeof job.deliverable === 'string' ? job.deliverable : (job.deliverable ?? null),
  };
}

function isMissingNotificationMemoError(err) {
  const message = String(err?.message || err || '');
  return message.includes('No notification memo found');
}

async function waitForJobCompletion(acpClient, jobId) {
  const deadline = Date.now() + JOIN_TIMEOUT_MS;
  let autoPaid = false;
  while (Date.now() < deadline) {
    const job = await acpClient.getJobById(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found while polling`);
    }

    if (job.phase === AcpJobPhases.NEGOTIATION && !autoPaid) {
      process.stderr.write(`Job ${jobId}: NEGOTIATION, approving ACP payment if required...\n`);
      try {
        await job.payAndAcceptRequirement();
        autoPaid = true;
      } catch (err) {
        if (!isMissingNotificationMemoError(err)) {
          throw err;
        }
        process.stderr.write(
          `Job ${jobId}: transaction memo not ready yet, retrying...\n`,
        );
        await sleep(2_000);
        continue;
      }
      await sleep(2_000);
      continue;
    }

    if (job.phase === AcpJobPhases.COMPLETED) {
      return job;
    }

    if (job.phase === AcpJobPhases.REJECTED) {
      throw new Error(`Job ${jobId} rejected${job.rejectionReason ? `: ${job.rejectionReason}` : ''}`);
    }

    if (job.phase === AcpJobPhases.EXPIRED) {
      throw new Error(`Job ${jobId} expired`);
    }

    process.stderr.write(`Job ${jobId}: ${phaseName(job.phase)}\n`);
    await sleep(JOIN_POLL_INTERVAL_MS);
  }

  throw new Error(`Timed out waiting for job ${jobId}`);
}

async function waitForJobSubmission(acpClient, jobId) {
  const deadline = Date.now() + 60_000;
  let autoPaid = false;
  let lastJob = null;
  while (Date.now() < deadline) {
    const job = await acpClient.getJobById(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found while polling`);
    }
    lastJob = job;

    if (job.phase === AcpJobPhases.NEGOTIATION && !autoPaid) {
      process.stderr.write(`Job ${jobId}: NEGOTIATION, approving ACP payment if required...\n`);
      try {
        await job.payAndAcceptRequirement();
        autoPaid = true;
      } catch (err) {
        if (!isMissingNotificationMemoError(err)) {
          throw err;
        }
        process.stderr.write(
          `Job ${jobId}: transaction memo not ready yet, retrying...\n`,
        );
        await sleep(2_000);
        continue;
      }
      await sleep(2_000);
      continue;
    }

    if (job.phase === AcpJobPhases.REJECTED) {
      throw new Error(`Job ${jobId} rejected${job.rejectionReason ? `: ${job.rejectionReason}` : ''}`);
    }

    if (job.phase === AcpJobPhases.EXPIRED) {
      throw new Error(`Job ${jobId} expired`);
    }

    if (
      autoPaid &&
      job.phase !== AcpJobPhases.NEGOTIATION &&
      job.phase !== AcpJobPhases.REQUEST
    ) {
      return job;
    }

    process.stderr.write(`Job ${jobId}: ${phaseName(job.phase)}\n`);
    await sleep(2_000);
  }

  if (lastJob) {
    return lastJob;
  }
  throw new Error(`Timed out waiting for job ${jobId} submission`);
}

async function resolveOffering(acpClient, providerAddress, jobOfferingName) {
  const provider = await acpClient.getAgent(providerAddress, { showHiddenOfferings: true });
  if (!provider) {
    throw new Error(`Provider ${providerAddress} not found / not available on selected ACP contract`);
  }
  const selected = provider.jobOfferings.find((offering) => offering.name === jobOfferingName);
  if (!selected) {
    const available = provider.jobOfferings.map((offering) => offering.name).join(', ') || '<none>';
    throw new Error(`Offering ${jobOfferingName} not found for ${providerAddress}. Available: ${available}`);
  }
  return { provider, selected };
}

function resolveFareAmount(selectedOffering, acpContractClient) {
  if (selectedOffering.priceType === 'percentage') {
    return new FareAmount(0, acpContractClient.config.baseFare);
  }
  const price = Number(selectedOffering.price || 0);
  if (!Number.isFinite(price) || price < 0) {
    throw new Error(`Invalid offering price ${selectedOffering.price}`);
  }
  return new FareAmount(price, acpContractClient.config.baseFare);
}

async function main() {
  loadDotEnv(ENV_PATH);

  const { command, options, dryRun, submitOnly } = readArgs(process.argv.slice(2));
  if (dryRun && !['job_status', 'job_resume', 'active_jobs'].includes(command)) {
    const requirement = buildRequirement(command, { ...options });
    const previewRequirement = command === 'join'
      ? { agentAddress: process.env.ACP_CLIENT_WALLET_ADDRESS || '<wallet-from-env>', publicKey: '<generated-rsa-public-key>' }
      : requirement;
    const previewPayload = buildJobPayload(command === 'join' ? 'join_leaderboard' : command, previewRequirement);
    console.log(JSON.stringify({
      status: 'dry_run',
      provider: DGCLAW_PROVIDER_ADDRESS,
      offering: command === 'join' ? 'join_leaderboard' : command,
      serviceRequirement: previewPayload,
    }, null, 2));
    return;
  }

  const env = getEnvConfig();
  const acpContractClient = await AcpContractClientV2.build(
    env.privateKey,
    env.sessionEntityKeyId,
    env.walletAddress,
    selectContractConfig(env.contractMode),
  );

  const acpClient = new AcpClientClass({
    acpContractClient,
    ...(env.customRpcUrl ? { customRpcUrl: env.customRpcUrl } : {}),
  });
  await acpClient.init();

  if (command === 'job_status') {
    const jobId = options.jobId;
    if (!jobId) {
      throw new Error('job_status requires --jobId');
    }
    const job = await acpClient.getJobById(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }
    console.log(JSON.stringify(normalizeJob(job), null, 2));
    return;
  }

  if (command === 'active_jobs') {
    const jobs = await acpClient.getActiveJobs(1, 100);
    console.log(JSON.stringify({
      jobs: (jobs || []).map(normalizeJob),
    }, null, 2));
    return;
  }

  if (command === 'job_resume') {
    const jobId = options.jobId;
    if (!jobId) {
      throw new Error('job_resume requires --jobId');
    }
    const job = submitOnly
      ? await waitForJobSubmission(acpClient, jobId)
      : await waitForJobCompletion(acpClient, jobId);
    const deliverable = submitOnly ? job.deliverable ?? null : await resolveDeliverable(acpClient, job.deliverable);
    console.log(JSON.stringify({
      status: submitOnly ? 'submitted' : 'completed',
      ...normalizeJob(job),
      deliverable,
    }, null, 2));
    return;
  }

  const requirement = buildRequirement(command, { ...options });

  if (command === 'join') {
    const joinKeys = generateRsaJoinKeyPair();
    const joinRequirement = {
      agentAddress: env.walletAddress,
      publicKey: joinKeys.publicKeyOneLine,
    };
    const { provider, selected } = await resolveOffering(acpClient, DGCLAW_PROVIDER_ADDRESS, 'join_leaderboard');
    const fareAmount = resolveFareAmount(selected, acpContractClient);
    const serviceRequirement = buildJobPayload('join_leaderboard', joinRequirement);
    const jobId = await acpClient.initiateJob(
      provider.walletAddress,
      serviceRequirement,
      fareAmount,
    );
    process.stderr.write(`Created join_leaderboard job ${jobId}. Waiting for completion...\n`);
    const completedJob = await waitForJobCompletion(acpClient, jobId);
    const deliverable = await resolveDeliverable(acpClient, completedJob.deliverable);
    const encryptedApiKey = deliverable && deliverable.encryptedApiKey;
    if (!encryptedApiKey) {
      throw new Error(`Job ${jobId} completed but did not return encryptedApiKey`);
    }
    const apiKey = decryptEncryptedApiKey(encryptedApiKey, joinKeys.privateKeyPem);
    if (!apiKey) {
      throw new Error(`Job ${jobId} completed but API key decryption failed`);
    }
    upsertEnvValue(ENV_PATH, 'DGCLAW_API_KEY', apiKey);
    console.log(JSON.stringify({
      status: 'joined',
      provider: provider.walletAddress,
      offering: 'join_leaderboard',
      jobId,
      phase: phaseName(completedJob.phase),
      dgclawApiKeySaved: true,
      dgclawApiKeyPath: ENV_PATH,
      agentAddress: env.walletAddress,
    }, null, 2));
    return;
  }

  const { provider, selected } = await resolveOffering(acpClient, DGCLAW_PROVIDER_ADDRESS, command);
  const fareAmount = resolveFareAmount(selected, acpContractClient);
  const serviceRequirement = buildJobPayload(command, requirement);
  const jobId = await acpClient.initiateJob(
    provider.walletAddress,
    serviceRequirement,
    fareAmount,
  );
  if (submitOnly) {
    process.stderr.write(`Created ${command} job ${jobId}. Waiting for submission...\n`);
    const submittedJob = await waitForJobSubmission(acpClient, jobId);
    console.log(JSON.stringify({
      status: 'submitted',
      provider: provider.walletAddress,
      offering: command,
      price: selected.price,
      priceType: selected.priceType,
      fareAmount: String(fareAmount.amount),
      fareToken: fareAmount.fare.contractAddress,
      serviceRequirement,
      jobId,
      phase: phaseName(submittedJob.phase),
      paymentRequestData: submittedJob.paymentRequestData || null,
      rejectionReason: submittedJob.rejectionReason || null,
      deliverable: submittedJob.deliverable || null,
    }, null, 2));
    return;
  }

  process.stderr.write(`Created ${command} job ${jobId}. Waiting for completion...\n`);
  const completedJob = await waitForJobCompletion(acpClient, jobId);
  const deliverable = await resolveDeliverable(acpClient, completedJob.deliverable);

  console.log(JSON.stringify({
    status: 'completed',
    provider: provider.walletAddress,
    offering: command,
    price: selected.price,
    priceType: selected.priceType,
    fareAmount: String(fareAmount.amount),
    fareToken: fareAmount.fare.contractAddress,
    serviceRequirement,
    ...summarizeCompletedJob(completedJob, deliverable),
  }, null, 2));
}

main()
  .then(() => {
    process.exit(0);
  })
  .catch((err) => {
    console.error(`Error creating ACP job: ${err?.message || err}`);
    process.exit(1);
  });
