import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtemp, mkdir, rm, utimes, writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {scanRuns} from './report-data.js';

function validNodeMetrics(overrides = {}) {
  return {
    memory: {start_bytes: 2187089488, maximum_bytes: 2187153288},
    network: {send_octets: 3195},
    schedulers: {utilization_percent: 0.98, maximum_run_queue_length: 2},
    ...overrides
  };
}

function validMetrics() {
  return {
    node1: validNodeMetrics(),
    node2: validNodeMetrics()
  };
}

function validPoint(overrides = {}) {
  return {
    path: 'elock',
    nodes: {node1: 'local', node2: 'runner@node2.example.net'},
    clients_per_node: 2,
    transactions_per_client: 3,
    transaction: {read: 0, update: 1, write: 0},
    objects_pool_size: 100,
    read_ms: 0,
    timeout: 'undefined',
    restart_ms: 0,
    seed: 12345,
    deadlocks: false,
    write_ms: 10,
    elapsed_ms: 36,
    transactions: 12,
    transactions_per_second: 325.73,
    locks: 12,
    locks_per_second: 325.73,
    lock_time_percent: 6.52,
    restarts: 0,
    metrics: validMetrics(),
    ...overrides
  };
}

function globalPoint(overrides = {}) {
  const point = validPoint({path: 'global', ...overrides});
  return point;
}

function privDirectory(root, run) {
  return path.join(
    root,
    run,
    'test.performance.performance_transaction_SUITE.logs',
    'run.2026-10-02_15.47.30',
    'log_private');
}

function fileContent(content) {
  return typeof content === 'string' ? content : JSON.stringify(content);
}

// A run directory as Common Test lays it out, with the given point files
async function writeRun(root, run, files) {
  const dataDirectory = path.join(privDirectory(root, run), 'performance_data');
  await mkdir(dataDirectory, {recursive: true});
  await Promise.all(Object.entries(files).map(([name, content]) =>
    writeFile(path.join(dataDirectory, name), fileContent(content))));
}

// The marker of the running point, next to performance_data
async function writeMarker(root, run, content) {
  const directory = privDirectory(root, run);
  await mkdir(directory, {recursive: true});
  await writeFile(
    path.join(directory, 'performance_running.json'),
    fileContent(content));
}

function validMarker(pid) {
  const {path: pointPath, nodes, clients_per_node, transactions_per_client,
    transaction, objects_pool_size, read_ms, timeout, restart_ms, seed, deadlocks,
    write_ms} = validPoint();
  return {
    path: pointPath,
    nodes,
    clients_per_node,
    transactions_per_client,
    transaction,
    objects_pool_size,
    read_ms,
    timeout,
    restart_ms,
    seed,
    deadlocks,
    write_ms,
    index: 7,
    total: 28,
    started_at: 1790946217245,
    os_pid: pid
  };
}

// The pid of a process that has exited
function deadPid() {
  return spawnSync(process.execPath, ['-e', '']).pid;
}

async function withRoot(prefix, body) {
  const temporaryRoot = await mkdtemp(
    path.join(os.tmpdir(), `elock-performance-report-${prefix}-`));
  try {
    await body(temporaryRoot);
  } finally {
    await rm(temporaryRoot, {recursive: true, force: true});
  }
}

function errorFields(run) {
  return run.errors.map(error => error.message).sort();
}

test('accepts a valid point of each path', async () => {
  await withRoot('valid', async (root) => {
    await writeRun(root, 'ct_run.valid', {
      'elock.2.1.100.0.json': validPoint(),
      'mnesia.2.1.100.0.json': validPoint({
        path: 'mnesia',
        deadlocks: true,
        restarts: 3
      }),
      'global.2.1.100.0.json': globalPoint()
    });

    const result = await scanRuns(root);

    assert.equal(result.runs.length, 1);
    assert.deepEqual(result.runs[0].errors, []);
    assert.deepEqual(
      result.runs[0].points.map(point => point.path).sort(),
      ['elock', 'global', 'mnesia']);
  });
});

test('reports malformed and invalid point files as run errors', async () => {
  await withRoot('invalid', async (root) => {
    await writeRun(root, 'ct_run.invalid', {
      'unfinished.json': '{unfinished',
      'empty.json': {},
      'path.json': validPoint({path: 'native'}),
      'transaction.json': validPoint({transaction: {read: 1, update: -1, write: 0}}),
      'clients.json': validPoint({clients_per_node: 0}),
      'rate.json': validPoint({locks_per_second: -1}),
      'nodes.json': validPoint({nodes: {}}),
      'location.json': validPoint({nodes: {node1: '', node2: 'local'}}),
      'elock-restarts.json': validPoint({restarts: undefined}),
      'run-queue.json': validPoint({
        metrics: {
          node1: validNodeMetrics(),
          node2: validNodeMetrics({
            schedulers: {utilization_percent: 1, maximum_run_queue_length: 2.5}
          })
        }
      })
    });

    const result = await scanRuns(root);
    const [run] = result.runs;

    assert.equal(run.points.length, 0);
    assert.equal(run.errors.length, 10);
    assert.ok(run.errors.some(error => error.file.endsWith('unfinished.json')));
    assert.deepEqual(
      errorFields(run).filter(
        message => message.startsWith('invalid performance point field:')),
      [
        'clients_per_node',
        'transaction.update',
        'locks_per_second',
        'metrics.node2.schedulers.maximum_run_queue_length',
        'nodes',
        'nodes.node1',
        'path',
        'path',
        'restarts'
      ].map(field => `invalid performance point field: ${field}`).sort());
  });
});

test('rejects a point without memory.start_bytes', async () => {
  await withRoot('memory', async (root) => {
    await writeRun(root, 'ct_run.memory', {
      'elock.2.1.100.0.json': validPoint({
        metrics: {
          node1: validNodeMetrics({memory: {maximum_bytes: 2189532208}}),
          node2: validNodeMetrics()
        }
      })
    });

    const result = await scanRuns(root);

    assert.equal(result.runs[0].points.length, 0);
    assert.deepEqual(errorFields(result.runs[0]), [
      'invalid performance point field: metrics.node1.memory.start_bytes'
    ]);
  });
});

test('rejects a point without a boolean deadlocks', async () => {
  await withRoot('deadlocks', async (root) => {
    // A point file of the format before the deadlocks setting
    const {deadlocks: _deadlocks, ...older} = validPoint();
    await writeRun(root, 'ct_run.deadlocks', {
      'older.json': older,
      'word.json': validPoint({deadlocks: 'false'}),
      'list.json': validPoint({deadlocks: [false, true]})
    });

    const result = await scanRuns(root);

    assert.equal(result.runs[0].points.length, 0);
    assert.deepEqual(errorFields(result.runs[0]), [
      'invalid performance point field: deadlocks',
      'invalid performance point field: deadlocks',
      'invalid performance point field: deadlocks'
    ]);
  });
});

test('requires transaction restarts for global', async () => {
  await withRoot('global', async (root) => {
    await writeRun(root, 'ct_run.global', {
      'global.2.1.100.0.json': validPoint({path: 'global', restarts: undefined})
    });

    const result = await scanRuns(root);

    assert.equal(result.runs[0].points.length, 0);
    assert.deepEqual(errorFields(result.runs[0]), [
      'invalid performance point field: restarts'
    ]);
  });
});

test('rejects metrics whose keys do not match the nodes', async () => {
  await withRoot('roles', async (root) => {
    await writeRun(root, 'ct_run.roles', {
      'missing.json': validPoint({metrics: {node1: validNodeMetrics()}}),
      'extra.json': validPoint({
        metrics: {...validMetrics(), node3: validNodeMetrics()}
      }),
      'renamed.json': validPoint({
        metrics: {node1: validNodeMetrics(), other: validNodeMetrics()}
      })
    });

    const result = await scanRuns(root);

    assert.equal(result.runs[0].points.length, 0);
    assert.deepEqual(errorFields(result.runs[0]), [
      'invalid performance point field: metrics',
      'invalid performance point field: metrics',
      'invalid performance point field: metrics'
    ]);
  });
});

test('leaves out runs without points or errors, newest run first', async () => {
  await withRoot('runs', async (root) => {
    await writeRun(root, 'ct_run.nonode@nohost.2026-10-02_15.35.39', {
      'elock.2.1.100.0.json': validPoint()
    });
    await writeRun(root, 'ct_run.nonode@nohost.2026-10-02_15.47.30', {
      'unfinished.json': '{unfinished'
    });
    // A functional run: Common Test logs without performance data
    await mkdir(
      path.join(root, 'ct_run.nonode@nohost.2026-10-02_16.00.00', 'log_private'),
      {recursive: true});
    // An empty performance_data directory
    await writeRun(root, 'ct_run.nonode@nohost.2026-10-02_16.10.00', {});

    const result = await scanRuns(root);

    assert.deepEqual(result.runs.map(({id}) => id), [
      'ct_run.nonode@nohost.2026-10-02_15.47.30',
      'ct_run.nonode@nohost.2026-10-02_15.35.39'
    ]);
    assert.equal(result.runs[0].errors.length, 1);
    assert.equal(result.runs[1].points.length, 1);
  });
});

test('adds the modification time of the file to a point as finished_at', async () => {
  await withRoot('finished', async (root) => {
    const run = 'ct_run.finished';
    await writeRun(root, run, {'2.1.0.100.elock.json': validPoint()});
    const finishedAt = new Date('2026-10-02T15:47:30.000Z');
    await utimes(
      path.join(privDirectory(root, run), 'performance_data', '2.1.0.100.elock.json'),
      finishedAt,
      finishedAt);

    const result = await scanRuns(root);

    assert.deepEqual(result.runs[0].errors, []);
    assert.equal(result.runs[0].points[0].finished_at, finishedAt.getTime());
  });
});

test('reports the running point of a live marker without its pid', async () => {
  await withRoot('running', async (root) => {
    await writeRun(root, 'ct_run.running', {'2.1.0.100.elock.json': validPoint()});
    await writeMarker(root, 'ct_run.running', validMarker(process.pid));

    const result = await scanRuns(root);
    const {os_pid: _pid, ...expected} = validMarker(process.pid);

    assert.deepEqual(result.runs[0].running, expected);
    assert.equal(result.runs[0].points.length, 1);
    assert.deepEqual(result.runs[0].errors, []);
  });
});

test('takes the marker of a dead process for not running', async () => {
  await withRoot('dead', async (root) => {
    await writeRun(root, 'ct_run.dead', {'2.1.0.100.elock.json': validPoint()});
    await writeMarker(root, 'ct_run.dead', validMarker(deadPid()));

    const result = await scanRuns(root);

    assert.equal(result.runs[0].running, null);
    assert.equal(result.runs[0].points.length, 1);
    assert.deepEqual(result.runs[0].errors, []);
  });
});

test('takes a malformed marker for not running, not for an error', async () => {
  await withRoot('malformed', async (root) => {
    await writeRun(root, 'ct_run.unfinished', {'2.1.0.100.elock.json': validPoint()});
    await writeMarker(root, 'ct_run.unfinished', '{unfinished');
    await writeRun(root, 'ct_run.no_pid', {'2.1.0.100.elock.json': validPoint()});
    await writeMarker(root, 'ct_run.no_pid', validMarker(undefined));
    await writeRun(root, 'ct_run.group_pid', {'2.1.0.100.elock.json': validPoint()});
    await writeMarker(root, 'ct_run.group_pid', validMarker(0));

    const result = await scanRuns(root);

    assert.equal(result.runs.length, 3);
    result.runs.forEach((run) => {
      assert.equal(run.running, null);
      assert.equal(run.points.length, 1);
      assert.deepEqual(run.errors, []);
    });
  });
});

test('keeps a run with only a live marker, leaves out one with a dead marker', async () => {
  await withRoot('marker-only', async (root) => {
    await writeMarker(root, 'ct_run.live', validMarker(process.pid));
    await writeMarker(root, 'ct_run.dead', validMarker(deadPid()));

    const result = await scanRuns(root);

    assert.deepEqual(result.runs.map(({id}) => id), ['ct_run.live']);
    assert.deepEqual(result.runs[0].points, []);
    assert.deepEqual(result.runs[0].errors, []);
    assert.equal(result.runs[0].running.index, 7);
  });
});

test('returns no runs when the Common Test log root does not exist', async () => {
  const result = await scanRuns('/no/such/elock/performance/logs');
  assert.deepEqual(result.runs, []);
});

 test('accepts zero costs and counts, undefined timeout, and integer seeds', async () => {
  await withRoot('zero', async root => {
    await writeRun(root, 'ct_run.zero', {
      'elock.json': validPoint({transaction: {read: 1, update: 0, write: 0}, write_ms: 0, seed: -12}),
      'global.json': globalPoint({timeout: 25, restarts: 2})
    });
    const {runs: [run]} = await scanRuns(root);
    assert.equal(run.points.length, 2);
    assert.deepEqual(run.errors, []);
  });
});
