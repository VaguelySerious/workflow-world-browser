import { eventIdToSlot, type Event, type WorkflowRun } from '@workflow/world';
import { createWorld } from 'workflow-world-browser';
import {
  approve,
  correlationId,
  createHandler,
  listEvents,
  PIPELINE,
  type ProgressLine,
  progressStream,
  QUEUE_PREFIX,
  startRun,
} from './runner.ts';
import './style.css';

const DB_NAME = 'wwb-demo';
/** The World's own consumer lock (see src/queue.ts in the package). */
const WORLD_LOCK = `workflow-world-browser:${DB_NAME}:queue`;
/** Gates handler registration so this page knows whether it is the driver. */
const DRIVER_LOCK = `${DB_NAME}:driver`;

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const esc = (s: unknown) =>
  String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]!);
const short = (value: unknown, max = 80) => {
  const text = value === undefined ? '' : JSON.stringify(value, (_k, v) => (v instanceof Uint8Array ? `<${v.length} bytes>` : v));
  return text.length > max ? `${text.slice(0, max)}…` : text;
};

const world = await createWorld({ name: DB_NAME });

// ---------------------------------------------------------------------------
// Driving. Every tab can start runs, resume hooks and read everything. Only
// the tab holding the driver lock registers the queue handler, which makes
// the World request its consumer lock and start delivering. When that tab
// closes or reloads, its locks are released and the next tab in line takes
// over, redelivering whatever message was in flight.
// ---------------------------------------------------------------------------
let driving = false;
let runningStep: { runId: string; task: string; attempt: number } | undefined;
const activity: string[] = [];
function log(message: string) {
  activity.unshift(`${new Date().toLocaleTimeString()}  ${message}`);
  activity.length = Math.min(activity.length, 60);
  renderActivity();
}

void navigator.locks.request(DRIVER_LOCK, async () => {
  driving = true;
  log('this tab acquired the Web Lock and is now driving the queue');
  world.createQueueHandler!(QUEUE_PREFIX, createHandler(world, { log, onStep: (s) => (runningStep = s) }));
  await world.start?.();
  renderRole();
  return new Promise<never>(() => {}); // hold until the tab goes away
});

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------
let selected: string | undefined = location.hash.slice(1) || undefined;
function select(runId: string | undefined) {
  if (runId === selected) return;
  selected = runId;
  history.replaceState(null, '', runId ? `#${runId}` : location.pathname + location.search);
  openStream();
  void refresh();
}

$('start').onclick = async () => select(await startRun(world));
$('approve').onclick = async () => {
  if (selected) await approve(world, selected);
};
$('crash').onclick = () => {
  // A reload kills this page's JavaScript on the spot, like a crash: no
  // cleanup runs, the in-flight queue message stays in IndexedDB.
  location.reload();
};
$('second').onclick = () => window.open(location.href, '_blank');
$('clear').onclick = async () => {
  if (!confirm('Delete every run, event, stream and queued message?')) return;
  await world.clear();
  select(undefined);
};
$('runs').onchange = (e) => select((e.target as HTMLSelectElement).value || undefined);

// ---------------------------------------------------------------------------
// Live progress: the run's stream, tailed through the World (IndexedDB rows
// plus BroadcastChannel hints, so it updates in every tab).
// ---------------------------------------------------------------------------
let lines: ProgressLine[] = [];
let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
async function openStream() {
  await reader?.cancel().catch(() => {});
  reader = undefined;
  lines = [];
  renderStream();
  const runId = selected;
  if (!runId) return;
  const stream = await world.streams.get(runId, progressStream(runId), 0);
  const mine = stream.getReader();
  reader = mine;
  const decoder = new TextDecoder();
  let buffer = '';
  for (;;) {
    const { value, done } = await mine.read().catch(() => ({ value: undefined, done: true }));
    if (done || reader !== mine) return;
    buffer += decoder.decode(value, { stream: true });
    let nl: number;
    while ((nl = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, nl);
      buffer = buffer.slice(nl + 1);
      if (line) lines.push(JSON.parse(line));
    }
    renderStream();
  }
}

// ---------------------------------------------------------------------------
// Polling the World (cheap: it is all local IndexedDB).
// ---------------------------------------------------------------------------
let rawDb: IDBDatabase | undefined;
function openRaw(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const r = indexedDB.open(DB_NAME);
    r.onsuccess = () => resolve(r.result);
    r.onerror = () => reject(r.error);
  });
}
interface QueuedRow {
  messageId: string;
  queueName: string;
  payload: { runId?: string };
  availableAt: number;
  attempt: number;
}
async function queueRows(): Promise<QueuedRow[]> {
  rawDb ??= await openRaw();
  return new Promise((resolve, reject) => {
    const r = rawDb!.transaction('queue').objectStore('queue').getAll();
    r.onsuccess = () => resolve(r.result as QueuedRow[]);
    r.onerror = () => reject(r.error);
  });
}

let runs: WorkflowRun[] = [];
let events: Event[] = [];
let run: WorkflowRun | undefined;
let queue: QueuedRow[] = [];
let locks: LockManagerSnapshot | undefined;

async function refresh() {
  const list = await world.runs.list({ pagination: { limit: 20 } } as never);
  runs = list.data as WorkflowRun[];
  if (!selected && runs[0]) {
    select(runs[0].runId);
    return;
  }
  run = selected ? runs.find((r) => r.runId === selected) : undefined;
  events = run ? await listEvents(world, run.runId) : [];
  queue = await queueRows();
  locks = await navigator.locks.query();
  render();
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------
function renderRole() {
  const el = $('role');
  el.className = driving ? 'role driving' : 'role standby';
  el.innerHTML = driving
    ? '<b>Driving.</b> This tab holds the Web Lock and delivers queue messages.'
    : '<b>Standby.</b> Another tab holds the Web Lock. This tab reads, starts runs and resumes hooks; the driving tab executes them. Close it and this tab takes over.';
}

function renderActivity() {
  $('activity').textContent = activity.join('\n') || '(nothing yet)';
}

function renderStream() {
  $('stream').textContent =
    lines
      .slice(-40)
      .map((l) => {
        const time = new Date(l.at).toLocaleTimeString();
        if (l.pct !== undefined) return `${time}  ${l.task.padEnd(16)} attempt ${l.attempt}  ${'#'.repeat(l.pct / 10).padEnd(10, '.')} ${l.pct}%`;
        return `${time}  ${l.task.padEnd(16)} ${l.note ?? ''}`;
      })
      .join('\n') || '(no progress yet)';
  const el = $('stream');
  el.scrollTop = el.scrollHeight;
}

let lastRunList = '';
function render() {
  // Run picker (only rebuilt when the list changes, so it stays usable).
  const key = runs.map((r) => `${r.runId}:${r.status}`).join(',') + selected;
  if (key !== lastRunList) {
    lastRunList = key;
    $('runs').innerHTML =
      '<option value="">(pick a run)</option>' +
      runs
        .map(
          (r) =>
            `<option value="${r.runId}" ${r.runId === selected ? 'selected' : ''}>${r.runId} (${r.status})</option>`
        )
        .join('');
  }

  // Pipeline
  const hookWaiting =
    !!run &&
    events.some((e) => e.eventType === 'hook_created') &&
    !events.some((e) => e.eventType === 'hook_received');
  ($('approve') as HTMLButtonElement).disabled = !hookWaiting;
  $('approve').classList.toggle('attention', hookWaiting);
  $('run-status').innerHTML = run
    ? `<span class="pill ${run.status}">${run.status}</span> <code>${run.runId}</code> input <code>${esc(short(run.input))}</code>`
    : 'No run selected. Press <b>Start a run</b>.';
  $('pipeline').innerHTML = run
    ? PIPELINE.map((task) => {
        const id = correlationId(task, run!.runId);
        const own = events.filter((e) => e.correlationId === id);
        const has = (t: string) => own.find((e) => e.eventType === t) as (Event & { eventData?: any }) | undefined;
        let state = 'pending';
        let detail = '';
        if (task.kind === 'step') {
          const starts = own.filter((e) => e.eventType === 'step_started').length;
          const done = has('step_completed');
          if (done) {
            state = 'done';
            detail = `result ${esc(short(done.eventData?.result, 60))}${starts > 1 ? ` · took ${starts} attempts` : ''}`;
          } else if (starts) {
            state = 'running';
            const last = [...lines].reverse().find((l) => l.task === task.name && l.attempt === starts);
            const here = runningStep?.runId === run!.runId && runningStep.task === task.name;
            detail = `attempt ${starts} · ${last?.pct ?? 0}%${starts > 1 ? ' · recovered after a crash' : ''}${
              here ? ' · executing in this tab' : ''
            }`;
          }
        } else if (task.kind === 'sleep') {
          const created = has('wait_created');
          if (has('wait_completed')) state = 'done';
          else if (created) {
            state = 'waiting';
            const left = Math.max(0, Math.ceil((new Date(created.eventData.resumeAt).getTime() - Date.now()) / 1000));
            detail = `${left}s left · a queue message is scheduled for resumeAt`;
          }
        } else {
          const received = has('hook_received');
          if (received) {
            state = 'done';
            detail = `payload ${esc(short(received.eventData?.payload, 60))}`;
          } else if (has('hook_created')) {
            state = 'waiting';
            detail = 'press <b>Approve</b> (works from any tab)';
          }
        }
        return `<li class="task ${state}"><span class="dot"></span><span class="label">${esc(task.label)}</span><span class="state">${state}</span><span class="detail">${detail}</span></li>`;
      }).join('')
    : '';

  // Event log
  $('events').innerHTML =
    events
      .map(
        (e) => `<tr class="${e.eventType.split('_')[0]}"><td>${eventIdToSlot(e.eventId)}</td><td>${esc(e.eventType)}</td><td><code>${esc(
          e.correlationId ?? ''
        )}</code></td><td>${new Date(e.createdAt).toLocaleTimeString()}</td><td><code>${esc(
          short((e as { eventData?: unknown }).eventData, 60)
        )}</code></td></tr>`
      )
      .join('') || '<tr><td colspan="5" class="empty">(no events)</td></tr>';

  // Queue
  const now = Date.now();
  $('queue').innerHTML =
    queue
      .sort((a, b) => a.availableAt - b.availableAt)
      .map((m) => {
        const due = m.availableAt <= now ? 'due now (in flight or next)' : `in ${Math.ceil((m.availableAt - now) / 1000)}s`;
        return `<tr><td><code>${esc(m.messageId.slice(-10))}</code></td><td><code>${esc(m.queueName)}</code></td><td><code>…${esc(
          (m.payload?.runId ?? '').slice(-6)
        )}</code></td><td>${m.attempt}</td><td>${due}</td></tr>`;
      })
      .join('') || '<tr><td colspan="5" class="empty">(empty: nothing to deliver)</td></tr>';

  // Locks
  const held = locks?.held?.map((l) => l.name) ?? [];
  const pending = locks?.pending?.map((l) => l.name) ?? [];
  $('locks').innerHTML = [WORLD_LOCK, DRIVER_LOCK]
    .map((name) => {
      const waiting = pending.filter((n) => n === name).length;
      return `<li><code>${esc(name)}</code>: ${held.includes(name) ? 'held' : 'free'}${
        waiting ? ` · ${waiting} tab(s) waiting` : ''
      }</li>`;
    })
    .join('');
}

renderRole();
renderActivity();
openStream();
await refresh();
setInterval(() => void refresh().catch((err) => console.error(err)), 400);
