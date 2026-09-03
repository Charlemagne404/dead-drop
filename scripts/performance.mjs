import { spawn, spawnSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import path from "node:path";

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(scriptDirectory, "..");
const manifest = path.join(repositoryRoot, "src-tauri", "Cargo.toml");
const profile = process.env.DROP_PERF_PROFILE === "debug" ? "debug" : "release";
const binary = path.join(repositoryRoot, "src-tauri", "target", profile, "performance_peer");
const requestedLargeBytes = Number.parseInt(
  process.env.DROP_PERF_LARGE_BYTES ?? `${256 * 1024 * 1024}`,
  10,
);
const requestedSmallFileCount = Number.parseInt(process.env.DROP_PERF_SMALL_FILE_COUNT ?? "64", 10);
const requestedSmallFileBytes = Number.parseInt(process.env.DROP_PERF_SMALL_FILE_BYTES ?? "4096", 10);
const requestedPeerCount = Number.parseInt(process.env.DROP_PERF_PEER_COUNT ?? "10000", 10);
const timeoutMs = Number.parseInt(process.env.DROP_PERF_TIMEOUT_MS ?? "120000", 10);
const jsonOnly = process.argv.includes("--json");

if (!Number.isSafeInteger(requestedLargeBytes) || requestedLargeBytes < 0) {
  throw new Error("DROP_PERF_LARGE_BYTES must be a non-negative safe integer");
}

const cases = [
  { name: "zero-bytes", size: 0, count: 1 },
  { name: "one-byte", size: 1, count: 1 },
  { name: "four-kib", size: 4 * 1024, count: 1 },
  { name: "one-mib", size: 1024 * 1024, count: 1 },
  { name: "one-hundred-mib", size: 100 * 1024 * 1024, count: 1 },
  { name: "larger-generated", size: requestedLargeBytes, count: 1 },
  { name: "many-small-files", size: requestedSmallFileBytes, count: requestedSmallFileCount },
];

const buildArgs = ["build", "--manifest-path", manifest, "--features", "integration-tests", "--bin", "performance_peer"];
if (profile === "release") buildArgs.push("--release");
const build = spawnSync(
  "cargo",
  buildArgs,
  { cwd: repositoryRoot, encoding: "utf8", stdio: jsonOnly ? ["ignore", "pipe", "pipe"] : "inherit" },
);
if (build.status !== 0) {
  if (jsonOnly) process.stderr.write(`${build.stdout ?? ""}${build.stderr ?? ""}`);
  process.exit(build.status ?? 1);
}

const results = [];
const registry = await runRegistry();
for (const benchmarkCase of cases) {
  const result = await runCase(benchmarkCase);
  results.push(result);
  if (!jsonOnly) printCase(result);
}

const report = {
  schema: 1,
  command: "npm run perf",
  host: { platform: process.platform, arch: process.arch, node: process.version },
  generated: {
    largerBytes: requestedLargeBytes,
    smallFileCount: requestedSmallFileCount,
    smallFileBytes: requestedSmallFileBytes,
    peerCount: requestedPeerCount,
  },
  registry,
  cases: results,
};

if (jsonOnly) {
  process.stdout.write(`${JSON.stringify(report)}\n`);
} else {
  process.stdout.write("\nPERF_JSON ");
  process.stdout.write(`${JSON.stringify(report)}\n`);
}

async function runCase(benchmarkCase) {
  const receiverSpawnedAt = performance.now();
  const receiver = spawn(binary, ["receiver"], { cwd: repositoryRoot, stdio: ["ignore", "pipe", "pipe"] });
  const receiverLines = collectLines(receiver);
  const receiverReady = await waitForMetric(receiverLines, "PERF_READY", timeoutMs);
  const receiverReadyMs = performance.now() - receiverSpawnedAt;
  const samples = { sender: [], receiver: [] };
  const sampler = setInterval(() => {
    sampleProcess(receiver.pid, samples.receiver);
  }, 100);
  const senderArgs = [
    "sender",
    "--address", receiverReady.address,
    "--id", receiverReady.id,
    "--name", receiverReady.name,
    "--os", receiverReady.os,
    "--size", String(benchmarkCase.size),
    "--count", String(benchmarkCase.count),
  ];
  const senderSpawnedAt = performance.now();
  const sender = spawn(binary, senderArgs, { cwd: repositoryRoot, stdio: ["ignore", "pipe", "pipe"] });
  const senderLines = collectLines(sender);
  const senderStartPromise = waitForMetric(senderLines, "PERF_SENDER_START", timeoutMs);
  const senderSampler = setInterval(() => {
    sampleProcess(sender.pid, samples.sender);
  }, 100);
  try {
    const senderStart = await senderStartPromise;
    const senderProcessToStartMs = performance.now() - senderSpawnedAt;
    const [senderResult, receiverRequest, receiverResult] = await Promise.all([
      waitForMetric(senderLines, "PERF_RESULT", timeoutMs),
      waitForMetric(receiverLines, "PERF_RECEIVER_REQUEST", timeoutMs),
      waitForMetric(receiverLines, "PERF_RESULT", timeoutMs),
    ]);
    await Promise.all([waitForExit(sender), waitForExit(receiver)]);
    return {
      ...benchmarkCase,
      receiver_process_ready_ms: receiverReadyMs,
      sender_process_to_start_ms: senderProcessToStartMs,
      sender: { ...senderResult, ...senderStart, ...summarizeSamples(samples.sender) },
      receiver: { ...receiverResult, request_ms: receiverRequest.request_ms, ...summarizeSamples(samples.receiver) },
      transfer_mbps: throughputMbps(benchmarkCase.size * benchmarkCase.count, senderResult.total_ms),
      accepted_transfer_mbps: throughputMbps(
        benchmarkCase.size * benchmarkCase.count,
        senderResult.accepted_to_terminal_ms,
      ),
      sha256_to_prepare_ratio_percent: percentage(
        senderStart.sha256_ms,
        senderResult.prepare_and_request_ms,
      ),
      progress_event_rate_hz: rate(senderResult.progress_events, senderResult.accepted_to_terminal_ms),
      frontend_progress_render_upper_bound_hz: rate(
        senderResult.update_events,
        senderResult.accepted_to_terminal_ms,
      ),
    };
  } catch (error) {
    terminate(receiver);
    terminate(sender);
    throw new Error(`${benchmarkCase.name}: ${error.message}`);
  } finally {
    clearInterval(sampler);
    clearInterval(senderSampler);
  }
}

async function runRegistry() {
  const result = spawnSync(binary, ["registry", String(requestedPeerCount)], {
    cwd: repositoryRoot,
    encoding: "utf8",
    timeout: timeoutMs,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`registry benchmark exited with ${result.status}: ${result.stderr}`);
  const line = result.stdout.split(/\r?\n/).find((value) => value.startsWith("PERF_REGISTRY "));
  if (!line) throw new Error("registry benchmark did not emit PERF_REGISTRY");
  return JSON.parse(line.slice("PERF_REGISTRY ".length));
}

function collectLines(child) {
  const lines = [];
  const reader = createInterface({ input: child.stdout });
  reader.on("line", (line) => {
    const space = line.indexOf(" ");
    if (space < 0) return;
    const kind = line.slice(0, space);
    if (!kind.startsWith("PERF_")) return;
    try {
      lines.push({ kind, value: JSON.parse(line.slice(space + 1)) });
    } catch {
      // Ignore non-JSON diagnostic lines so a benchmark failure can still time out cleanly.
    }
  });
  return lines;
}

async function waitForMetric(lines, kind, limitMs) {
  const started = Date.now();
  while (Date.now() - started < limitMs) {
    const metric = lines.find((entry) => entry.kind === kind);
    if (metric) return metric.value;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`timed out waiting for ${kind}`);
}

function waitForExit(child) {
  if (child.exitCode !== null) {
    if (child.exitCode !== 0) return Promise.reject(new Error(`benchmark process exited with ${child.exitCode}`));
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code) => code === 0 ? resolve() : reject(new Error(`benchmark process exited with ${code}`)));
  });
}

function sampleProcess(pid, samples) {
  if (!pid) return;
  const result = spawnSync("ps", ["-o", "%cpu=,rss=", "-p", String(pid)], { encoding: "utf8" });
  const values = (result.stdout ?? "").trim().split(/\s+/).map(Number);
  if (values.length === 2 && values.every(Number.isFinite)) {
    samples.push({ cpu_percent: values[0], rss_kib: values[1] });
  }
}

function summarizeSamples(samples) {
  return {
    cpu_percent_max: samples.length ? Math.max(...samples.map((sample) => sample.cpu_percent)) : null,
    rss_kib_max: samples.length ? Math.max(...samples.map((sample) => sample.rss_kib)) : null,
    samples: samples.length,
  };
}

function throughputMbps(bytes, milliseconds) {
  if (!bytes || !milliseconds) return 0;
  return bytes / 1024 / 1024 / (milliseconds / 1000);
}

function rate(count, milliseconds) {
  if (!count || !milliseconds) return 0;
  return count / (milliseconds / 1000);
}

function percentage(part, whole) {
  if (!part || !whole) return 0;
  return (part / whole) * 100;
}

function terminate(child) {
  if (child && child.exitCode === null) child.kill("SIGTERM");
}

function printCase(result) {
  const sender = result.sender;
  const receiver = result.receiver;
  process.stdout.write(
    `${result.name.padEnd(22)} ${String(result.size * result.count).padStart(12)} B ` +
    `request ${sender.prepare_and_request_ms.toFixed(1).padStart(8)} ms ` +
    `total ${sender.total_ms.toFixed(1).padStart(8)} ms ` +
    `throughput ${result.transfer_mbps.toFixed(1).padStart(8)} MiB/s ` +
    `sha ${sender.sha256_ms.toFixed(1).padStart(8)} ms ` +
    `progress ${sender.progress_events}/${receiver.progress_events} ` +
    `events ${sender.update_events} ` +
    `cpu ${formatNumber(sender.cpu_percent_max)}%/${formatNumber(receiver.cpu_percent_max)}% ` +
    `rss ${formatNumber(sender.rss_kib_max)} / ${formatNumber(receiver.rss_kib_max)} KiB\n`,
  );
}

function formatNumber(value) {
  return value === null ? "—" : value.toFixed(1);
}
