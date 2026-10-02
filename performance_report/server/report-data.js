import {readdir, readFile, stat} from 'node:fs/promises';
import path from 'node:path';

const paths = new Set(['elock', 'mnesia', 'global']);
const positiveIntegerFields = [
  'clients_per_node',
  'transactions_per_client',
  'locks_per_transaction',
  'write_ms',
  'transactions',
  'locks'
];
const percentFields = ['exclusive_percent', 'intersect_percent'];
const rateFields = [
  'transactions_per_second',
  'locks_per_second',
  'lock_time_percent'
];

function requireValue(condition, field) {
  if (!condition) {
    throw new Error(`invalid performance point field: ${field}`);
  }
}

function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isNonNegativeNumber(value) {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}

function isNonNegativeInteger(value) {
  return Number.isInteger(value) && value >= 0;
}

function isPositiveInteger(value) {
  return Number.isInteger(value) && value > 0;
}

function isPercent(value) {
  return Number.isInteger(value) && value >= 0 && value <= 100;
}

function validatePoint(point) {
  requireValue(isObject(point), 'root');
  requireValue(paths.has(point.path), 'path');
  positiveIntegerFields.forEach(field =>
    requireValue(isPositiveInteger(point[field]), field));
  percentFields.forEach(field =>
    requireValue(isPercent(point[field]), field));
  requireValue(typeof point.deadlocks === 'boolean', 'deadlocks');
  validateNodes(point.nodes);
  requireValue(isNonNegativeInteger(point.elapsed_ms), 'elapsed_ms');
  rateFields.forEach(field =>
    requireValue(isNonNegativeNumber(point[field]), field));
  // global retries internally: its points carry no restarts
  requireValue(
    point.path === 'global'
      ? point.restarts === undefined
      : isNonNegativeInteger(point.restarts),
    'restarts');
  validateMetrics(point.metrics, Object.keys(point.nodes));
  return point;
}

function validateNodes(nodes) {
  requireValue(isObject(nodes) && Object.keys(nodes).length > 0, 'nodes');
  Object.entries(nodes).forEach(([role, location]) =>
    requireValue(
      typeof location === 'string' && location.length > 0,
      `nodes.${role}`));
}

// The metrics are keyed by exactly the roles of the nodes
function validateMetrics(metrics, roles) {
  requireValue(isObject(metrics), 'metrics');
  const keys = Object.keys(metrics);
  requireValue(
    keys.length === roles.length && roles.every(role => keys.includes(role)),
    'metrics');
  roles.forEach(role => validateNodeMetrics(metrics[role], `metrics.${role}`));
}

function validateNodeMetrics(node, field) {
  requireValue(isObject(node), field);
  requireValue(isObject(node.memory), `${field}.memory`);
  requireValue(
    isNonNegativeNumber(node.memory.start_bytes),
    `${field}.memory.start_bytes`);
  requireValue(
    isNonNegativeNumber(node.memory.maximum_bytes),
    `${field}.memory.maximum_bytes`);
  requireValue(isObject(node.network), `${field}.network`);
  requireValue(
    isNonNegativeNumber(node.network.send_octets),
    `${field}.network.send_octets`);
  requireValue(isObject(node.schedulers), `${field}.schedulers`);
  requireValue(
    isNonNegativeNumber(node.schedulers.utilization_percent),
    `${field}.schedulers.utilization_percent`);
  requireValue(
    isNonNegativeInteger(node.schedulers.maximum_run_queue_length),
    `${field}.schedulers.maximum_run_queue_length`);
}

const markerName = 'performance_running.json';

// The files of a run the report reads: the points and the markers of
// the running point
async function reportFiles(directory) {
  const entries = await readdir(directory, {withFileTypes: true});
  const files = await Promise.all(entries.map(async (entry) => {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      return reportFiles(entryPath);
    }
    if (entry.isFile() && entry.name.endsWith('.json') &&
        path.basename(directory) === 'performance_data' &&
        directory.split(path.sep).includes('log_private')) {
      return [{point: entryPath}];
    }
    if (entry.isFile() && entry.name === markerName &&
        path.basename(directory) === 'log_private') {
      return [{marker: entryPath}];
    }
    return [];
  }));
  return files.flat();
}

// The point files do not carry finished_at: it is the time the suite
// wrote the file
async function parsePoint(file, runDirectory) {
  const relativeFile = path.relative(runDirectory, file);
  try {
    const point = JSON.parse(await readFile(file, 'utf8'));
    const info = await stat(file);
    return {
      point: {...validatePoint(point), finished_at: Math.round(info.mtimeMs)},
      error: null
    };
  } catch (error) {
    return {
      point: null,
      error: {file: relativeFile, message: error.message}
    };
  }
}

// A process of this host: a signal 0 reaches it or is not permitted
function isAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === 'EPERM';
  }
}

// The point the suite runs now, or null. The marker of a killed run
// stays on disk: its process is dead. A marker that can not be read or
// parsed is not running either
async function readRunning(file) {
  try {
    const {os_pid: pid, ...running} = JSON.parse(await readFile(file, 'utf8'));
    // A pid of 0 or below would signal a process group
    return Number.isInteger(pid) && pid > 0 && isAlive(pid) ? running : null;
  } catch {
    return null;
  }
}

async function scanRun(logsRoot, entry) {
  const runDirectory = path.join(logsRoot, entry.name);
  const files = await reportFiles(runDirectory);
  const parsed = await Promise.all(files.flatMap(
    ({point}) => point === undefined ? [] : [parsePoint(point, runDirectory)]));
  const markers = await Promise.all(files.flatMap(
    ({marker}) => marker === undefined ? [] : [readRunning(marker)]));
  const info = await stat(runDirectory);
  return {
    id: entry.name,
    modified_at: info.mtime.toISOString(),
    report_url: `/ct-logs/${encodeURIComponent(entry.name)}/index.html`,
    points: parsed.flatMap(({point}) => point === null ? [] : [point]),
    errors: parsed.flatMap(({error}) => error === null ? [] : [error]),
    running: markers.find((marker) => marker !== null) ?? null
  };
}

export async function scanRuns(logsRoot) {
  let entries;
  try {
    entries = await readdir(logsRoot, {withFileTypes: true});
  } catch (error) {
    if (error.code === 'ENOENT') {
      return {logs_root: logsRoot, runs: []};
    }
    throw error;
  }

  const runEntries = entries.filter(
    (entry) => entry.isDirectory() && entry.name.startsWith('ct_run.'));
  const scanned = await Promise.all(
    runEntries.map((entry) => scanRun(logsRoot, entry)));
  // The functional suites write their runs to the same logs root. A run
  // that has not finished its first point yet has only the marker
  const runs = scanned.filter((run) =>
    run.points.length > 0 || run.errors.length > 0 || run.running !== null);
  runs.sort((left, right) => right.id.localeCompare(left.id));
  return {logs_root: logsRoot, runs};
}
