import {memo, useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip
} from 'chart.js';
import {Line} from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip
);

const REFRESH_INTERVAL_MS = 5000;
const DARK_SCHEME = '(prefers-color-scheme: dark)';

// The series, always in this order; each has its color in styles.css
const paths = ['elock', 'mnesia', 'global'];

const dimensions = [
  {id: 'clients_per_node', label: 'Clients / node'},
  {id: 'locks_per_transaction', label: 'Locks / transaction'},
  {id: 'intersect_percent', label: 'Intersect %'},
  {id: 'exclusive_percent', label: 'Exclusive %'}
];
const clientsDimension = dimensions[0];

const views = [
  {id: 'slice', label: 'Slice'},
  {id: 'groups', label: 'Groups'}
];

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
  const point = run.points[0] ?? run.running ?? undefined;
  if (point === undefined) {
    return {
      nodes: 'Unavailable',
      transactionsPerClient: 'Unavailable',
      writeMs: 'Unavailable',
      deadlocks: 'Unavailable'
    };
  }
  return {
    nodes: Object.entries(point.nodes)
      .map(([role, location]) => `${role}: ${location}`)
      .join(', '),
    transactionsPerClient: point.transactions_per_client.toLocaleString(),
    writeMs: point.write_ms.toLocaleString(),
    // The order of the locks of a transaction
    deadlocks: point.deadlocks ? 'true (random)' : 'false (sorted)'
  };
}

function pathsPresent(points) {
  return paths.filter(path => points.some(point => point.path === path));
}

// The values of a dimension present in the points, ascending
function dimensionValues(points, dimension) {
  return [...new Set(points.map(point => point[dimension]))]
    .sort((left, right) => left - right);
}

function valueAt(points, path, xDimension, xValue, metric) {
  const point = points.find(candidate =>
    candidate.path === path && candidate[xDimension] === xValue);
  const value = point === undefined ? undefined : metric.value(point);
  return Number.isFinite(value) ? value : null;
}

function formatValue(value, unit) {
  if (value === null) return 'N/A';
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
      <td>{config.nodes}</td>
      <td>{config.transactionsPerClient}</td>
      <td>{config.writeMs}</td>
      <td>{config.deadlocks}</td>
      <td>{present.length === 0 ? 'None' : present.join(', ')}</td>
    </tr>
  );
}

function MetricGrid({points, xDimension, xValues}) {
  return (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th rowSpan={2}>Metric</th>
            <th rowSpan={2} className="path-name">Path</th>
            <th colSpan={xValues.length} className="dimension-name">{xDimension.label}</th>
          </tr>
          <tr>
            {xValues.map(value => <th key={value}>{value.toLocaleString()}</th>)}
          </tr>
        </thead>
        <tbody>
          {metrics.flatMap(metric => paths.map((path, pathIndex) => (
            <tr
              className={pathIndex === paths.length - 1 ? 'metric-row-end' : undefined}
              key={`${metric.id}.${path}`}
            >
              {pathIndex === 0 && (
                <th className="metric-name" rowSpan={paths.length}>
                  {metric.label}
                </th>
              )}
              <th className="path-name">
                <span className={`path-marker ${path}`} />{path}
              </th>
              {xValues.map(value => (
                <td key={value}>
                  {formatValue(valueAt(points, path, xDimension.id, value, metric), metric.unit)}
                </td>
              ))}
            </tr>
          )))}
        </tbody>
      </table>
    </div>
  );
}

//--------------------------------------------------------------------
// The name of every visible series just right of its last point, in
// the muted text color. Labels closer than LABEL_GAP px are pushed
// down, the chart keeps LABEL_PADDING px on the right for them
//--------------------------------------------------------------------
const LABEL_GAP = 12;
const LABEL_OFFSET = 8;
const LABEL_PADDING = 56;

const directLabels = {
  id: 'directLabels',
  afterDatasetsDraw(chart, _arguments, options) {
    const labels = chart.data.datasets
      .flatMap((dataset, index) => {
        const last = dataset.data.findLastIndex(value => value !== null);
        if (last === -1 || !chart.isDatasetVisible(index)) return [];
        const {x, y} = chart.getDatasetMeta(index).data[last];
        return [{text: dataset.label, x, y}];
      })
      .sort((left, right) => left.y - right.y);
    labels.forEach((label, index) => {
      const above = labels[index - 1];
      if (above !== undefined && label.y - above.y < LABEL_GAP) {
        label.y = above.y + LABEL_GAP;
      }
    });
    const {ctx} = chart;
    ctx.save();
    ctx.font = `${options.size}px ${ChartJS.defaults.font.family}`;
    ctx.fillStyle = options.color;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    labels.forEach(({text, x, y}) => ctx.fillText(text, x + LABEL_OFFSET, y));
    ctx.restore();
  }
};

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

function ChartCanvas({points, xDimension, xValues, metric, theme}) {
  const data = {
    labels: xValues.map(value => value.toLocaleString()),
    datasets: paths.map(path => ({
      label: path,
      data: xValues.map(value => valueAt(points, path, xDimension.id, value, metric)),
      borderColor: theme.series[path],
      backgroundColor: theme.series[path],
      borderWidth: 2,
      pointRadius: 4,
      pointHoverRadius: 5,
      spanGaps: false
    }))
  };
  const axis = {
    grid: {color: theme.grid, lineWidth: 1},
    border: {color: theme.grid},
    ticks: {color: theme.ticks}
  };
  const options = {
    animation: false,
    maintainAspectRatio: false,
    layout: {padding: {right: LABEL_PADDING}},
    interaction: {mode: 'index', intersect: false},
    plugins: {
      directLabels: {color: theme.ticks, size: 11},
      legend: {position: 'bottom', labels: {color: theme.ticks}},
      tooltip: {
        callbacks: {
          label: context =>
            `${context.dataset.label}: ${formatValue(context.parsed.y, metric.unit)}`
        }
      }
    },
    scales: {
      x: {
        ...axis,
        type: 'category',
        title: {display: true, text: xDimension.label, color: theme.ticks}
      },
      y: {
        ...axis,
        // A count has integer ticks
        ticks: metric.count ? {...axis.ticks, precision: 0} : axis.ticks,
        title: {display: true, text: metric.count ? 'count' : metric.unit, color: theme.ticks},
        beginAtZero: false
      }
    }
  };
  return <Line data={data} options={options} plugins={[directLabels]} />;
}

//--------------------------------------------------------------------
// The pieces of both views: the table and a chart per metric of the
// points over the x dimension
//--------------------------------------------------------------------
function PointsView({points, xDimension, xValues, theme}) {
  return (
    <>
      <MetricGrid points={points} xDimension={xDimension} xValues={xValues} />
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
            xDimension={xDimension}
            xValues={xValues}
            metric={metric}
            theme={theme}
          />
        ))}
      </div>
    </>
  );
}

function RunConstants({config, dateTime}) {
  return (
    <dl className="config">
      <div><dt>Transactions / client</dt><dd>{config.transactionsPerClient}</dd></div>
      <div><dt>Write</dt><dd>{config.writeMs} ms</dd></div>
      <div><dt>Deadlocks</dt><dd>{config.deadlocks}</dd></div>
      <div><dt>Date/time</dt><dd>{dateTime}</dd></div>
    </dl>
  );
}

//--------------------------------------------------------------------
// Groups: a block per (locks, intersect, exclusive), x = clients,
// from the block that got a point last to the oldest
//--------------------------------------------------------------------
function groupPoints(points) {
  const groups = new Map();
  points.forEach(point => {
    const key = [
      point.locks_per_transaction,
      point.intersect_percent,
      point.exclusive_percent
    ].join('|');
    if (!groups.has(key)) {
      groups.set(key, {
        key,
        locks: point.locks_per_transaction,
        intersect: point.intersect_percent,
        exclusive: point.exclusive_percent,
        finishedAt: point.finished_at,
        points: []
      });
    }
    const group = groups.get(key);
    group.finishedAt = Math.max(group.finishedAt, point.finished_at);
    group.points.push(point);
  });
  // The block with the newest point first
  return [...groups.values()].sort((left, right) =>
    right.finishedAt - left.finishedAt ||
    left.locks - right.locks ||
    left.intersect - right.intersect ||
    left.exclusive - right.exclusive);
}

function GroupsView({run, config, dateTime, theme}) {
  const groups = useMemo(() => groupPoints(run.points), [run.points]);
  return groups.map(group => (
    <section className="point-group" key={group.key}>
      <header>
        <div>
          <span className="eyebrow">Group</span>
          <h3>
            {`${group.locks.toLocaleString()} lock(s) / transaction, `}
            {`${group.intersect}% intersect, ${group.exclusive}% exclusive`}
          </h3>
        </div>
        <RunConstants config={config} dateTime={dateTime} />
      </header>
      <PointsView
        points={group.points}
        xDimension={clientsDimension}
        xValues={dimensionValues(group.points, clientsDimension.id)}
        theme={theme}
      />
    </section>
  ));
}

//--------------------------------------------------------------------
// Slice: an x dimension, the other three fixed. A fixed dimension
// defaults to its largest value
//--------------------------------------------------------------------
function defaultSlice(values) {
  return {
    x: clientsDimension.id,
    fixed: Object.fromEntries(dimensions
      .filter(({id}) => id !== clientsDimension.id)
      .map(({id}) => [id, values[id].at(-1)]))
  };
}

// The dimension the new x replaces becomes fixed at its default
function changeX(slice, values, x) {
  const {[x]: _replaced, ...fixed} = slice.fixed;
  return {x, fixed: {...fixed, [slice.x]: values[slice.x].at(-1)}};
}

function SliceView({run, config, dateTime, slice, onSlice, theme}) {
  const values = useMemo(() => Object.fromEntries(dimensions.map(({id}) =>
    [id, dimensionValues(run.points, id)])), [run.points]);
  const current = slice ?? defaultSlice(values);
  const xDimension = dimensions.find(({id}) => id === current.x);
  const points = run.points.filter(point =>
    Object.entries(current.fixed).every(([id, value]) => point[id] === value));
  return (
    <section className="point-group">
      <header>
        <div className="filters">
          <label>
            X axis
            <select
              value={current.x}
              onChange={event => onSlice(changeX(current, values, event.target.value))}
            >
              {dimensions.map(({id, label}) => <option key={id} value={id}>{label}</option>)}
            </select>
          </label>
          {dimensions.filter(({id}) => id !== current.x).map(({id, label}) => (
            <label key={id}>
              {label}
              <select
                value={current.fixed[id]}
                onChange={event => onSlice({
                  ...current,
                  fixed: {...current.fixed, [id]: Number(event.target.value)}
                })}
              >
                {values[id].map(value => (
                  <option key={value} value={value}>{value.toLocaleString()}</option>
                ))}
              </select>
            </label>
          ))}
        </div>
        <RunConstants config={config} dateTime={dateTime} />
      </header>
      <PointsView
        points={points}
        xDimension={xDimension}
        xValues={values[current.x]}
        theme={theme}
      />
    </section>
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
    plural(running.locks_per_transaction, 'lock'),
    `${running.intersect_percent}% intersect`,
    `${running.exclusive_percent}% exclusive`
  ].join(' · ');
  return (
    <div className="running">
      <span className="eyebrow">Running now</span>
      <p className="running-point">{point}</p>
      <p className="running-progress">
        point {running.index.toLocaleString()} of {running.total.toLocaleString()}
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

//--------------------------------------------------------------------
// Memoized: a refresh that leaves the run, the view, its slice and the
// theme as they were renders nothing of it again
//--------------------------------------------------------------------
const RunSection = memo(function RunSection({run, anchor, view, slice, onSlice, theme}) {
  const config = runConfig(run);
  const dateTime = runDateTime(run);
  let content;
  if (run.points.length === 0) {
    content = <div className="empty"><h3>No completed points in this run</h3></div>;
  } else if (view === 'slice') {
    content = (
      <SliceView
        run={run}
        config={config}
        dateTime={dateTime}
        slice={slice}
        onSlice={next => onSlice(run.id, next)}
        theme={theme}
      />
    );
  } else {
    content = <GroupsView run={run} config={config} dateTime={dateTime} theme={theme} />;
  }
  return (
    <section className="run-section" id={anchor}>
      <header className="run-header">
        <h2>Performance run</h2>
        <a href={run.report_url}>Open Common Test report ↗</a>
      </header>
      {run.running && <RunningBanner running={run.running} />}
      <RunErrors errors={run.errors} />
      {content}
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
  const [view, setView] = useState('slice');
  // The slice of every run the user has changed, by run id
  const [slices, setSlices] = useState({});
  const changeSlice = useCallback(
    (runId, slice) => setSlices(current => ({...current, [runId]: slice})),
    []);
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
          <div className="segmented" role="group" aria-label="View">
            {views.map(({id, label}) => (
              <button
                type="button"
                key={id}
                aria-pressed={view === id}
                onClick={() => setView(id)}
              >
                {label}
              </button>
            ))}
          </div>
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
                    <th>Transactions / client</th>
                    <th>Write (ms)</th>
                    <th>Deadlocks</th>
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
              view={view}
              slice={slices[run.id]}
              onSlice={changeSlice}
              theme={theme}
            />
          ))}
        </>}
    </main>
  );
}
