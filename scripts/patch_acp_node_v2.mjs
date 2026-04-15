#!/usr/bin/env node
import fs from "fs";
import path from "path";

const DIST_ROOT = path.resolve(
  path.dirname(new URL(import.meta.url).pathname),
  "../node_modules/@virtuals-protocol/acp-node-v2/dist"
);

function patchFile(filePath) {
  const original = fs.readFileSync(filePath, "utf8");
  let updated = original;

  updated = updated.replace(
    /(from\s+["'])(\.\.?(?:\/[^"']+)+)(["'])/g,
    (full, prefix, specifier, suffix) =>
      /\.[a-z]+$/i.test(specifier) ? full : `${prefix}${specifier}.js${suffix}`
  );

  updated = updated.replace(
    /(import\s*\(\s*["'])(\.\.?(?:\/[^"']+)+)(["']\s*\))/g,
    (full, prefix, specifier, suffix) =>
      /\.[a-z]+$/i.test(specifier) ? full : `${prefix}${specifier}.js${suffix}`
  );

  if (updated !== original) {
    fs.writeFileSync(filePath, updated, "utf8");
  }
}

function walk(dir) {
  if (!fs.existsSync(dir)) {
    return;
  }
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walk(fullPath);
    } else if (entry.isFile() && fullPath.endsWith(".js")) {
      patchFile(fullPath);
    }
  }
}

walk(DIST_ROOT);
