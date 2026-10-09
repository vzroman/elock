import {memo, useEffect, useRef, useState} from 'react';
import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  BarElement,
  Tooltip
} from 'chart.js';
import {Bar} from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  Legend,
  LinearScale,
  BarElement,
  Tooltip
);

const REFRESH_INTERVAL_MS = 5000;
const DARK_SCHEME = '(prefers-color-scheme: dark)';

// The series, always in this order; each has its color in styles.css
const paths = ['elock', 'mnesia', 'global'];

function nodeMetrics(point) {
  return Object.values(point.metrics);
}

function sum(values) {
  return values.reduce((total, value) => total + value, 0);
}

const metrics = [
  {id: 'throughput', label: 'Throughput', unit: 'tx/s', value: point => point.transactions_per_second},
  {id: 'locks', label: 'Locks', unit: 'locks/s', value: point => point.locks_per_second},
  {id: 'lock_time', label: 'Lock time share', unit: '%', value: point => point.lock_time_percent},
  {id: 'restarts', label: 'Restarts', unit: '', count: true, value: point => point.restarts},
  {id: 'elapsed', label: 'Elapsed time', unit: 's', value: point => point.elapsed_ms / 1000},
  {
    id: 'memory_growth', label: 'Memory growth', unit: 'GB',
    value: point => Math.max(...nodeMetrics(point).map(({memory}) =>
      (memory.maximum_bytes - memory.start_bytes) / 1_000_000_000))
  },
  {
    id: 'scheduler_utilization', label: 'Scheduler utilization', unit: '%',
    value: point => {
      const nodes = nodeMetrics(point);
      return sum(nodes.map(({schedulers}) => schedulers.utilization_percent)) / nodes.length;
    }
  },
  {
    id: 'run_queue_max', label: 'Max run queue', unit: '', count: true,
    value: point => Math.max(...nodeMetrics(point).map(({schedulers}) =>
      schedulers.maximum_run_queue_length))
  },
  {
    id: 'network', label: 'Network', unit: 'MB/s',
    value: point => sum(nodeMetrics(point).map(({network}) => network.send_octets)) /
      (point.elapsed_ms / 1000) / 1_000_000
  }
];

function runAnchor(run) {
  return `run-${run.id}`;
}

function runDateTime(run) {
  const match = run.id.match(/\d{4}-\d{2}-\d{2}_\d{2}\.\d{2}\.\d{2}$/);
  return match?.[0] ?? run.id;
}

// The configuration every point of a run shares; the marker of the
// running point carries it as well
function runConfig(run) {
  return run.points[0] ?? run.running;
}

function transactionLabel(transaction) {
  return `${transaction.read} read / ${transaction.update} update / ${transaction.write} write`;
}

function pathsPresent(points) {
  return paths.filter(path => points.some(point => point.path === path));
}

function formatValue(value, unit) {
  if (!Number.isFinite(value)) return 'N/A';
  const maximumFractionDigits = unit === 'GB' ? 3 : 2;
  const formatted = new Intl.NumberFormat(
    undefined,
    {maximumFractionDigits}
  ).format(value);
  return unit === '' ? formatted : `${formatted} ${unit}`;
}

//--------------------------------------------------------------------
// The chart colors of the active scheme, read from the custom
// properties of styles.css
//--------------------------------------------------------------------
function readChartTheme() {
  const style = window.getComputedStyle(document.documentElement);
  const color = name => style.getPropertyValue(name).trim();
  return {
    series: Object.fromEntries(paths.map(path => [path, color(`--${path}`)])),
    grid: color('--grid'),
    ticks: color('--muted')
  };
}

function useChartTheme() {
  const [theme, setTheme] = useState(readChartTheme);
  useEffect(() => {
    const scheme = window.matchMedia(DARK_SCHEME);
    const update = () => setTheme(readChartTheme());
    scheme.addEventListener('change', update);
    return () => scheme.removeEventListener('change', update);
  }, []);
  return theme;
}

function RunRow({run}) {
  const config = runConfig(run);
  const present = pathsPresent(run.points);
  return (
    <tr>
      <th scope="row">
        <a href={`#${runAnchor(run)}`}>{runDateTime(run)}</a>
        {run.running && <span className="tag">running</span>}
      </th>
      <td>{config ? Object.keys(config.nodes).join(', ') : 'Unavailable'}</td>
      <td>{config ? transactionLabel(config.transaction) : 'Unavailable'}</td>
      <td>{config?.clients_per_node.toLocaleString() ?? 'Unavailable'}</td>
      <td>{present.length === 0 ? 'None' : present.join(', ')}</td>
    </tr>
  );
}

function MetricGrid({points}) {
  return (
    <div className="table-scroll">
      <table>
        <thead><tr><th>Metric</th>{points.map(point => (
          <th key={point.path}><span className={`path-marker ${point.path}`} />{point.path}</th>
        ))}</tr></thead>
        <tbody>{metrics.map(metric => (
          <tr key={metric.id}>
            <th>{metric.label}</th>
            {points.map(point => <td key={point.path}>{formatValue(metric.value(point), metric.unit)}</td>)}
          </tr>
        ))}</tbody>
      </table>
    </div>
  );
}

//--------------------------------------------------------------------
// A chart is mounted only while its card is within a screen of the
// viewport; out of range the box keeps its height, empty
//--------------------------------------------------------------------
function useNearViewport() {
  const ref = useRef(null);
  const [near, setNear] = useState(false);
  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => setNear(entry.isIntersecting),
      {rootMargin: '100% 0px'});
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);
  return [ref, near];
}

function MetricChart(props) {
  const [ref, near] = useNearViewport();
  return (
    <article className="chart-card">
      <h4>{props.metric.label}</h4>
      <div className="chart" ref={ref}>{near && <ChartCanvas {...props} />}</div>
    </article>
  );
}

function ChartCanvas({points, metric, theme}) {
  const data = {
    labels: points.map(point => point.path),
    datasets: [{
      label: metric.label,
      data: points.map(point => metric.value(point)),
      backgroundColor: points.map(point => theme.series[point.path])
    }]
  };
  const axis = {
    grid: {color: theme.grid},
    border: {color: theme.grid},
    ticks: {color: theme.ticks}
  };
  const options = {
    animation: false,
    maintainAspectRatio: false,
    plugins: {
      legend: {display: false},
      tooltip: {callbacks: {label: context => formatValue(context.parsed.y, metric.unit)}}
    },
    scales: {
      x: axis,
      y: {...axis, beginAtZero: true,
        ticks: metric.count ? {...axis.ticks, precision: 0} : axis.ticks,
        title: {display: true, text: metric.count ? 'count' : metric.unit, color: theme.ticks}}
    }
  };
  return <Bar data={data} options={options} />;
}

function PointsView({points, theme}) {
  return (
    <>
      <MetricGrid points={points} />
      <p className="table-note">
        Over the nodes: memory growth is the largest growth of a node, scheduler
        utilization the mean, max run queue the largest, network the sum of the
        octets sent per second.
      </p>
      <div className="charts">
        {metrics.map(metric => (
          <MetricChart
            key={metric.id}
            points={points}
            metric={metric}
            theme={theme}
          />
        ))}
      </div>
    </>
  );
}

function RunConstants({config, dateTime}) {
  if (!config) return null;
  return (
    <>
      <dl className="config">
        <div><dt>Nodes</dt><dd>{Object.entries(config.nodes).map(([role, location]) => `${role}: ${location}`).join(', ')}</dd></div>
        <div><dt>Clients / node</dt><dd>{config.clients_per_node.toLocaleString()}</dd></div>
        <div><dt>Transactions / client</dt><dd>{config.transactions_per_client.toLocaleString()}</dd></div>
        <div><dt>Transaction</dt><dd>{transactionLabel(config.transaction)}</dd></div>
        <div><dt>Object pool</dt><dd>{config.objects_pool_size.toLocaleString()}</dd></div>
        <div><dt>Read cost</dt><dd>{config.read_ms} ms</dd></div>
        <div><dt>Write cost / operation</dt><dd>{config.write_ms} ms</dd></div>
        <div><dt>Timeout (elock/global)</dt><dd>{config.timeout === 'undefined' ? 'No limit' : `${config.timeout} ms`}</dd></div>
        <div><dt>Restart delay (elock/global)</dt><dd>{config.restart_ms} ms</dd></div>
        <div><dt>Think time between transactions</dt><dd>{config.think_ms} ms</dd></div>
        <div><dt>Seed</dt><dd>{config.seed}</dd></div>
        <div><dt>Deadlocks</dt><dd>{config.deadlocks ? 'true (random order)' : 'false (sorted order)'}</dd></div>
        <div><dt>Date/time</dt><dd>{dateTime}</dd></div>
      </dl>
      <p className="table-note">Mnesia uses its native retries and backoff; configured timeout and restart delay do not apply.
        Lock time is measured directly across all attempts and excludes read, commit, unlock and restart work.
        Think time is outside both transaction and lock measurements.</p>
      {config.skipped_paths?.length > 0 && <p className="table-note">
        Skipped: {config.skipped_paths.join(', ')}. Global requires zero reads, sorted order, and the configured lock capacity limit.
      </p>}
    </>
  );
}

//--------------------------------------------------------------------
// The point the suite runs now. The elapsed time ticks on a timer of
// its own: the report does not change while a point runs
//--------------------------------------------------------------------
function plural(count, unit) {
  return `${count.toLocaleString()} ${count === 1 ? unit : `${unit}s`}`;
}

function formatDuration(milliseconds) {
  const seconds = Math.max(0, Math.floor(milliseconds / 1000));
  const parts = [
    [Math.floor(seconds / 3600), 'h'],
    [Math.floor(seconds / 60) % 60, 'min']
  ].filter(([value]) => value > 0);
  return [...parts, [seconds % 60, 's']]
    .map(([value, unit]) => `${value} ${unit}`)
    .join(' ');
}

function Elapsed({since}) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  return <span>{formatDuration(now - since)}</span>;
}

function RunningBanner({running}) {
  const point = [
    running.path,
    `${plural(running.clients_per_node, 'client')} / node`,
    transactionLabel(running.transaction),
    `${running.objects_pool_size.toLocaleString()} objects`,
    `seed ${running.seed}`
  ].join(' · ');
  return (
    <div className="running">
      <span className="eyebrow">Running now</span>
      <p className="running-point">{point}</p>
      <p className="running-progress">
        path {running.index.toLocaleString()} of {running.total.toLocaleString()}
        {' · '}started {new Date(running.started_at).toLocaleTimeString()}
        {' · '}<Elapsed since={running.started_at} />
      </p>
    </div>
  );
}

function RunErrors({errors}) {
  if (errors.length === 0) return null;
  return (
    <details className="alert" open>
      <summary>{errors.length} point file(s) could not be parsed</summary>
      <ul>
        {errors.map(error => (
          <li key={error.file}><code>{error.file}</code>: {error.message}</li>
        ))}
      </ul>
    </details>
  );
}

const RunSection = memo(function RunSection({run, anchor, theme}) {
  const config = runConfig(run);
  return (
    <section className="run-section" id={anchor}>
      <header className="run-header">
        <h2>Performance run</h2>
        <a href={run.report_url}>Open Common Test report ↗</a>
      </header>
      {run.running && <RunningBanner running={run.running} />}
      <RunErrors errors={run.errors} />
      <RunConstants config={config} dateTime={runDateTime(run)} />
      {run.points.length === 0
        ? <div className="empty"><h3>No completed paths in this run</h3></div>
        : <PointsView points={[...run.points].sort((a, b) => paths.indexOf(a.path) - paths.indexOf(b.path))} theme={theme} />}
    </section>
  );
});

function EmptyState() {
  return (
    <div className="empty">
      <h2>No performance runs found</h2>
      <p>Run the Common Test performance suite; this page refreshes automatically.</p>
    </div>
  );
}

export default function App() {
  const [report, setReport] = useState({runs: []});
  const [loadError, setLoadError] = useState(null);
  const [updatedAt, setUpdatedAt] = useState(null);
  const theme = useChartTheme();
  // The last response: an identical one keeps the report as it is
  const lastText = useRef(null);

  async function refresh() {
    try {
      const response = await fetch('/api/report');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const text = await response.text();
      if (text !== lastText.current) {
        lastText.current = text;
        setReport(JSON.parse(text));
      }
      setLoadError(null);
      setUpdatedAt(new Date());
    } catch (error) {
      setLoadError(error.message);
    }
  }

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, REFRESH_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <main>
      <header className="page-header">
        <div>
          <span className="eyebrow">Common Test metrics</span>
          <h1>elock performance</h1>
          <p>elock, mnesia and global compared on imitated database transactions.</p>
        </div>
        <div className="toolbar">
          <button type="button" onClick={refresh}>Refresh</button>
        </div>
      </header>

      <div className="status">
        <span>{updatedAt ? `Updated ${updatedAt.toLocaleTimeString()}` : 'Loading…'}</span>
        <span>{report.runs.length} run(s)</span>
      </div>

      {loadError && <div className="alert">Could not load report data: {loadError}</div>}

      {report.runs.length === 0
        ? <EmptyState />
        : <>
          <nav className="contents" aria-label="Performance runs">
            <h2>Runs</h2>
            <div className="table-scroll">
              <table className="runs-grid">
                <thead>
                  <tr>
                    <th>Date/time</th>
                    <th>Nodes</th>
                    <th>Transaction</th>
                    <th>Clients / node</th>
                    <th>Paths present</th>
                  </tr>
                </thead>
                <tbody>
                  {report.runs.map(run => (
                    <RunRow key={run.id} run={run} />
                  ))}
                </tbody>
              </table>
            </div>
          </nav>
          {report.runs.map(run => (
            <RunSection
              key={run.id}
              run={run}
              anchor={runAnchor(run)}
              theme={theme}
            />
          ))}
        </>}
    </main>
  );
}
