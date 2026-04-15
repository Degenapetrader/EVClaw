#!/usr/bin/env node
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import {
  AcpAgent,
  AcpApiClient,
  PrivyAlchemyEvmProviderAdapter,
} from "@virtuals-protocol/acp-node-v2";
import { base } from "viem/chains";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENV_PATH = path.resolve(__dirname, "../.env");
const LEGACY_DGCLAW_PROVIDER_ADDRESS = "0xd478a8B40372db16cA8045F28C6FE07228F3781A";
const DEFAULT_CHAIN = base;
const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;

function usage() {
  return `Usage:
  node scripts/dgclaw_acp_job_v2.mjs whoami
  node scripts/dgclaw_acp_job_v2.mjs provider
  node scripts/dgclaw_acp_job_v2.mjs active_jobs
  node scripts/dgclaw_acp_job_v2.mjs job_status --jobId 123
  node scripts/dgclaw_acp_job_v2.mjs join

Required env:
  ACP_V2_WALLET_ID
  ACP_V2_AGENT_WALLET_ADDRESS
  ACP_V2_SIGNER_PRIVATE_KEY

Optional env:
  ACP_V2_CHAIN_ID=8453
  ACP_V2_DGCLAW_PROVIDER_ADDRESS=<override-v2-provider-wallet>
`;
}

function parseEnvLine(line) {
  const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
  if (!match) return null;
  let value = match[2];
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    value = value.slice(1, -1);
  }
  return { key: match[1], value };
}

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  const raw = fs.readFileSync(filePath, "utf8");
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const parsed = parseEnvLine(trimmed);
    if (parsed && !(parsed.key in process.env)) {
      process.env[parsed.key] = parsed.value;
    }
  }
}

function readArgs(argv) {
  if (!argv.length || argv.includes("--help") || argv.includes("-h")) {
    console.log(usage());
    process.exit(0);
  }
  const command = argv.shift();
  const options = {};
  for (let i = 0; i < argv.length; i++) {
    const item = argv[i];
    if (!item.startsWith("--")) {
      throw new Error(`Unknown arg: ${item}`);
    }
    const eq = item.indexOf("=");
    let key;
    let value;
    if (eq === -1) {
      key = item.slice(2);
      value = argv[++i];
    } else {
      key = item.slice(2, eq);
      value = item.slice(eq + 1);
    }
    if (value === undefined) {
      throw new Error(`Expected value for ${item}`);
    }
    options[key] = value;
  }
  return { command, options };
}

function getEnvConfig() {
  const walletId = process.env.ACP_V2_WALLET_ID?.trim();
  const walletAddress = process.env.ACP_V2_AGENT_WALLET_ADDRESS?.trim();
  const signerPrivateKey = process.env.ACP_V2_SIGNER_PRIVATE_KEY?.trim();
  const chainId = Number(process.env.ACP_V2_CHAIN_ID || DEFAULT_CHAIN.id);

  const missing = [
    !walletId && "ACP_V2_WALLET_ID",
    !walletAddress && "ACP_V2_AGENT_WALLET_ADDRESS",
    !signerPrivateKey && "ACP_V2_SIGNER_PRIVATE_KEY",
  ].filter(Boolean);
  if (missing.length) {
    throw new Error(`Missing required ACP v2 env: ${missing.join(", ")}`);
  }
  if (!Number.isInteger(chainId) || chainId <= 0) {
    throw new Error("ACP_V2_CHAIN_ID must be a positive integer");
  }
  return {
    walletId,
    walletAddress,
    signerPrivateKey,
    chainId,
  };
}

function generateRsaJoinKeyPair() {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: { type: "spki", format: "pem" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" },
  });
  const publicKeyOneLine = publicKey
    .split("\n")
    .filter((line) => line && !line.startsWith("---"))
    .join("");
  return { publicKeyOneLine, privateKeyPem: privateKey };
}

function decryptEncryptedApiKey(encryptedApiKey, privateKeyPem) {
  const ciphertext = Buffer.from(encryptedApiKey, "base64");
  return crypto.privateDecrypt(
    {
      key: privateKeyPem,
      padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
      oaepHash: "sha256",
    },
    ciphertext
  ).toString("utf8");
}

function upsertEnvValue(filePath, key, value) {
  const line = `${key}=${value}`;
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, `${line}\n`, "utf8");
    return;
  }
  const lines = fs.readFileSync(filePath, "utf8").split("\n");
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
  fs.writeFileSync(
    filePath,
    `${next.filter((entry, idx, arr) => !(idx === arr.length - 1 && entry === "")).join("\n")}\n`,
    "utf8"
  );
}

function statusName(status) {
  const names = {
    0: "REQUEST",
    1: "NEGOTIATION",
    2: "TRANSACTION",
    3: "EVALUATION",
    4: "COMPLETED",
    5: "REJECTED",
  };
  return names[status] ?? String(status);
}

async function createAgent(env) {
  const chain = env.chainId === base.id ? base : DEFAULT_CHAIN;
  const provider = await PrivyAlchemyEvmProviderAdapter.create({
    walletAddress: env.walletAddress,
    walletId: env.walletId,
    signerPrivateKey: env.signerPrivateKey,
    chains: [chain],
  });
  const agent = await AcpAgent.create({
    provider,
    api: new AcpApiClient(),
  });
  return { agent, chain };
}

async function ensureApiToken(agent) {
  const api = agent.getApi();
  await api.ensureAuthenticated();
  return api.token;
}

async function resolveDeliverable(agent, deliverable) {
  if (!deliverable) return null;
  if (typeof deliverable !== "string") return deliverable;
  if (!deliverable.startsWith("http")) {
    try {
      return JSON.parse(deliverable);
    } catch {
      return deliverable;
    }
  }
  const token = await ensureApiToken(agent);
  const response = await fetch(deliverable, {
    headers: { authorization: `Bearer ${token}` },
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch deliverable: ${response.status} ${response.statusText}`);
  }
  const payload = await response.json();
  const content = payload?.data?.content ?? payload?.data ?? null;
  if (typeof content === "string") {
    try {
      return JSON.parse(content);
    } catch {
      return content;
    }
  }
  return content;
}

function normalizeJob(job) {
  if (!job) return null;
  return {
    jobId: job.onChainJobId ?? job.jobId ?? null,
    phase: statusName(job.jobStatus),
    description: job.description ?? null,
    requirement: job.requirement ?? null,
    deliverable: job.deliverable ?? null,
    rejectionReason: job.rejectionReason ?? null,
    clientAddress: job.clientAddress ?? null,
    providerAddress: job.providerAddress ?? null,
    evaluatorAddress: job.evaluatorAddress ?? null,
    chainId: job.chainId ?? null,
  };
}

async function searchProviders(agent, offeringName) {
  const queries = ["degenclaw", "degen", "ng"];
  const matches = [];
  for (const query of queries) {
    const results = await agent.browseAgents(query, { topK: 50, showHidden: true });
    for (const candidate of results) {
      const offerings = candidate.offerings || [];
      if (
        offeringName
          ? offerings.some((offering) => offering.name === offeringName)
          : offerings.some((offering) =>
              ["perp_trade", "perp_modify", "perp_deposit", "perp_withdraw"].includes(
                offering.name
              )
            )
      ) {
        matches.push(candidate);
      }
    }
    if (matches.length) {
      break;
    }
  }
  return matches;
}

async function getProvider(agent, offeringName) {
  const explicitAddress =
    process.env.ACP_V2_DGCLAW_PROVIDER_ADDRESS?.trim() || LEGACY_DGCLAW_PROVIDER_ADDRESS;
  if (explicitAddress) {
    const direct = await agent.getAgentByWalletAddress(explicitAddress).catch(() => null);
    if (
      direct &&
      (!offeringName || direct.offerings.some((offering) => offering.name === offeringName))
    ) {
      return direct;
    }
  }

  const matches = await searchProviders(agent, offeringName);
  if (!matches.length) {
    throw new Error(
      offeringName
        ? `No ACP v2 provider found exposing offering ${offeringName}`
        : "No ACP v2 Degen provider found"
    );
  }
  return matches[0];
}

async function waitForJob(agent, jobId, { submitOnly = false, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  let targetJobId = String(jobId);
  let settled = false;
  let timer;

  const result = await new Promise((resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`Timed out waiting for job ${targetJobId}`));
    }, timeoutMs);

    agent.on("entry", async (session, entry) => {
      if (settled) return;
      if (String(session.jobId) !== targetJobId) return;
      try {
        if (entry.kind !== "system") {
          return;
        }
        switch (entry.event.type) {
          case "budget.set":
            await session.fund();
            break;
          case "job.funded":
            if (submitOnly) {
              settled = true;
              const job = await session.fetchJob();
              resolve({ status: "submitted", job });
            }
            break;
          case "job.submitted":
            if (submitOnly) {
              settled = true;
              const job = await session.fetchJob();
              resolve({ status: "submitted", job });
            } else {
              await session.complete("Approved");
            }
            break;
          case "job.completed": {
            settled = true;
            const job = await session.fetchJob();
            resolve({ status: "completed", job });
            break;
          }
          case "job.rejected":
            settled = true;
            reject(new Error(`Job ${targetJobId} rejected: ${entry.event.reason}`));
            break;
          case "job.expired":
            settled = true;
            reject(new Error(`Job ${targetJobId} expired`));
            break;
          default:
            break;
        }
      } catch (err) {
        settled = true;
        reject(err);
      }
    });
  }).finally(() => {
    clearTimeout(timer);
  });

  return result;
}

async function runJoin(agent, chainId, walletAddress) {
  const provider = await getProvider(agent, "join_leaderboard");
  const joinKeys = generateRsaJoinKeyPair();
  await agent.start();
  try {
    const jobId = await agent.createJobByOfferingName(
      chainId,
      "join_leaderboard",
      provider.walletAddress,
      {
        agentAddress: walletAddress,
        publicKey: joinKeys.publicKeyOneLine,
      },
      { evaluatorAddress: await agent.getAddress() }
    );
    process.stderr.write(`Created join_leaderboard job ${jobId}. Waiting for completion...\n`);
    const { job } = await waitForJob(agent, jobId);
    const deliverable = await resolveDeliverable(agent, job.deliverable);
    const encryptedApiKey = deliverable?.encryptedApiKey;
    if (!encryptedApiKey) {
      throw new Error(`Job ${jobId} completed but did not return encryptedApiKey`);
    }
    const apiKey = decryptEncryptedApiKey(encryptedApiKey, joinKeys.privateKeyPem);
    upsertEnvValue(ENV_PATH, "DGCLAW_API_KEY_V2", apiKey);
    return {
      status: "joined",
      jobId: String(jobId),
      phase: statusName(job.status),
      agentAddress: walletAddress,
      dgclawApiKeySaved: true,
      dgclawApiKeyPath: ENV_PATH,
      deliverable,
    };
  } finally {
    await agent.stop();
  }
}

async function main() {
  loadDotEnv(ENV_PATH);
  const { command, options } = readArgs(process.argv.slice(2));
  const env = getEnvConfig();
  const { agent, chain } = await createAgent(env);

  if (command === "whoami") {
    const provider = await getProvider(agent);
    console.log(
      JSON.stringify(
        {
          walletAddress: await agent.getAddress(),
          walletId: env.walletId,
          chainId: chain.id,
          providerFound: true,
          providerWalletAddress: provider.walletAddress,
          offerings: provider.offerings.map((offering) => offering.name),
        },
        null,
        2
      )
    );
    return;
  }

  if (command === "provider") {
    const provider = await getProvider(agent);
    console.log(
      JSON.stringify(
        {
          walletAddress: provider.walletAddress,
          name: provider.name,
          offerings: provider.offerings.map((offering) => ({
            name: offering.name,
            priceType: offering.priceType,
            priceValue: offering.priceValue,
            slaMinutes: offering.slaMinutes,
            requiredFunds: offering.requiredFunds,
          })),
        },
        null,
        2
      )
    );
    return;
  }

  if (command === "active_jobs") {
    const jobs = await agent.getApi().getActiveJobs();
    const detailed = [];
    for (const job of jobs) {
      const detail = await agent.getApi().getJob(job.chainId, job.onChainJobId);
      if (detail) {
        detailed.push(normalizeJob(detail));
      }
    }
    console.log(JSON.stringify({ jobs: detailed }, null, 2));
    return;
  }

  if (command === "job_status") {
    const jobId = options.jobId;
    if (!jobId) {
      throw new Error("job_status requires --jobId");
    }
    const job = await agent.getApi().getJob(chain.id, jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }
    console.log(JSON.stringify(normalizeJob(job), null, 2));
    return;
  }

  if (command === "join") {
    const result = await runJoin(agent, chain.id, env.walletAddress);
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  throw new Error(`Unsupported command: ${command}`);
}

main().catch((err) => {
  console.error(`ACP v2 error: ${err?.message || err}`);
  process.exit(1);
});
