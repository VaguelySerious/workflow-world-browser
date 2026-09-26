/**
 * A tiny durable workflow runner written directly against the World
 * interface. It is NOT the Workflow SDK runtime (see README.md for why): it
 * replays a fixed pipeline from the run's event log on every queue delivery.
 *
 * The protocol it follows is the one the SDK runtime uses on any World:
 *
 * - `run_created`, then a queue message per wake-up; the first delivery
 *   writes `run_started`.
 * - A step starts with a born-running `step_started` (the World materializes
 *   the step and a synthetic `step_created`). That event is the durable
 *   intent: a `step_started` with no `step_completed` after it means the tab
 *   running the step died, and the next delivery runs it again as attempt 2.
 * - A completed step's result lives in its `step_completed` event, so it is
 *   never re-run, and later steps read it from the log.
 * - A sleep is `wait_created` plus a queue redelivery at `resumeAt` (the
 *   handler returns `{ timeoutSeconds }`), then `wait_completed`.
 * - A hook is `hook_created` with a token. The delivery ends idle; resuming
 *   writes `hook_received` and enqueues a new wake-up.
 *
 * The queue deletes a message only after its delivery returns, so a tab
 * closed mid-step leaves the message for whichever tab drives next.
 */
import type { Event, QueuePrefix, ValidQueueName, World } from '@workflow/world';
import { monotonicFactory } from 'ulid';

const ulid = monotonicFactory();

export const QUEUE_PREFIX = '__wkf_workflow_' as QueuePrefix;
export const QUEUE_NAME = '__wkf_workflow_demo' as ValidQueueName;
export const WORKFLOW_NAME = 'demo/fulfillOrder';

export type Task =
  | { kind: 'step'; name: string; label: string; ms: number; run: (prior: Record<string, unknown>) => unknown }
  | { kind: 'sleep'; name: string; label: string; seconds: number }
  | { kind: 'hook'; name: string; label: string };

const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)]!;

/** The workflow: four ~2s steps, a durable sleep, and a hook a human resumes. */
export const PIPELINE: Task[] = [
  {
    kind: 'step',
    name: 'reserveInventory',
    label: 'Reserve inventory',
    ms: 2000,
    run: () => ({ sku: 'WWB-042', warehouse: pick(['SFO', 'IAD', 'FRA']) }),
  },
  {
    kind: 'step',
    name: 'chargeCard',
    label: 'Charge card',
    ms: 2500,
    run: () => ({ chargeId: `ch_${Math.random().toString(36).slice(2, 10)}`, amount: '$42.00' }),
  },
  { kind: 'sleep', name: 'coolingOff', label: 'Sleep 8s (cancellation window)', seconds: 8 },
  { kind: 'hook', name: 'approval', label: 'Wait for approval (hook)' },
  {
    kind: 'step',
    name: 'shipOrder',
    label: 'Ship order',
    ms: 2000,
    run: (prior) => ({
      tracking: `1Z${Math.random().toString(36).slice(2, 12).toUpperCase()}`,
      from: (prior.reserveInventory as { warehouse?: string } | undefined)?.warehouse,
    }),
  },
  {
    kind: 'step',
    name: 'sendReceipt',
    label: 'Send receipt',
    ms: 1500,
    run: (prior) => ({ sentFor: (prior.chargeCard as { chargeId?: string } | undefined)?.chargeId }),
  },
];

/** Correlation ids. Hook ids are global in the World, so they carry the run id. */
export function correlationId(task: Task, runId: string): string {
  if (task.kind === 'step') return `step_${task.name}`;
  if (task.kind === 'sleep') return `wait_${task.name}`;
  return `hook_${runId.slice(5)}_${task.name}`;
}
export const hookToken = (runId: string) => `approve:${runId}`;
export const progressStream = (runId: string) => `progress-${runId}`;

export interface ProgressLine {
  task: string;
  attempt?: number;
  pct?: number;
  note?: string;
  at: number;
}

export async function listEvents(world: World, runId: string): Promise<Event[]> {
  const out: Event[] = [];
  let cursor: string | null | undefined;
  for (;;) {
    const page = await world.events.list({
      runId,
      pagination: { sortOrder: 'asc', limit: 1000, ...(cursor ? { cursor } : {}) },
    } as never);
    out.push(...page.data);
    if (!page.hasMore) return out;
    cursor = page.cursor;
  }
}

export async function startRun(world: World): Promise<string> {
  const runId = `wrun_${ulid()}`;
  await world.events.create(runId, {
    eventType: 'run_created',
    eventData: {
      deploymentId: 'browser',
      workflowName: WORKFLOW_NAME,
      input: { orderId: `ord_${Math.random().toString(36).slice(2, 8)}` },
    },
  } as never);
  await world.queue(QUEUE_NAME, { runId } as never, { idempotencyKey: `start:${runId}` } as never);
  return runId;
}

/** Resume the approval hook from any tab: record the payload, then wake the run. */
export async function approve(world: World, runId: string): Promise<void> {
  const hook = await world.hooks.getByToken(hookToken(runId));
  await world.events.create(
    runId,
    {
      eventType: 'hook_received',
      correlationId: hook.hookId,
      eventData: { payload: { approved: true, at: new Date().toISOString() } },
    } as never,
    // (runId, resumeId) dedup: a double click records one resume.
    { resumeId: 'approve' } as never
  );
  await world.queue(QUEUE_NAME, { runId } as never);
}

export interface DriverHooks {
  /** A human-readable line for the "this tab" activity log. */
  log(message: string): void;
  /** Called when this tab starts or finishes executing a step body. */
  onStep?(running: { runId: string; task: string; attempt: number } | undefined): void;
}

/** The queue handler. Deliveries for one run are serialized within the tab. */
export function createHandler(world: World, hooks: DriverHooks) {
  const chains = new Map<string, Promise<unknown>>();
  return async (message: unknown, meta: { attempt: number; messageId: string }) => {
    const { runId } = message as { runId: string };
    const previous = chains.get(runId) ?? Promise.resolve();
    const next = previous.catch(() => {}).then(() => drive(world, runId, meta, hooks));
    chains.set(runId, next);
    try {
      return await next;
    } finally {
      if (chains.get(runId) === next) chains.delete(runId);
    }
  };
}

async function drive(
  world: World,
  runId: string,
  meta: { attempt: number; messageId: string },
  hooks: DriverHooks
): Promise<{ timeoutSeconds: number } | undefined> {
  const short = runId.slice(-6);
  hooks.log(`delivery ${meta.messageId.slice(-6)} (attempt ${meta.attempt}) for run …${short}`);
  const run = await world.runs.get(runId);
  if (run.status === 'pending') {
    await world.events.create(runId, { eventType: 'run_started' } as never);
  } else if (run.status !== 'running') {
    hooks.log(`run …${short} is ${run.status}; nothing to do`);
    return;
  }

  const events = await listEvents(world, runId);
  const byCorrelation = new Map<string, Event[]>();
  for (const e of events) {
    if (!e.correlationId) continue;
    const list = byCorrelation.get(e.correlationId) ?? [];
    list.push(e);
    byCorrelation.set(e.correlationId, list);
  }
  const stream = progressStream(runId);
  const progress = (line: Omit<ProgressLine, 'at'>) =>
    world.streams.write(runId, stream, `${JSON.stringify({ ...line, at: Date.now() })}\n`);

  const results: Record<string, unknown> = {};
  for (const task of PIPELINE) {
    const id = correlationId(task, runId);
    const log = byCorrelation.get(id) ?? [];
    const has = (type: string) => log.find((e) => e.eventType === type) as (Event & { eventData?: any }) | undefined;

    if (task.kind === 'step') {
      const done = has('step_completed');
      if (done) {
        results[task.name] = done.eventData?.result;
        continue;
      }
      const starts = log.filter((e) => e.eventType === 'step_started').length;
      const attempt = starts + 1;
      if (starts > 0) {
        hooks.log(`recovering ${task.name}: it started (attempt ${starts}) but never completed; running attempt ${attempt}`);
      }
      // First attempt: born running (the World creates the step). Later
      // attempts: plain step_started on the existing step.
      await world.events.create(runId, {
        eventType: 'step_started',
        correlationId: id,
        eventData: has('step_created') ? {} : { stepName: task.name, input: { prior: results } },
      } as never);
      hooks.onStep?.({ runId, task: task.name, attempt });
      hooks.log(`running ${task.name} (attempt ${attempt})`);
      const ticks = 10;
      for (let i = 1; i <= ticks; i++) {
        await new Promise((r) => setTimeout(r, task.ms / ticks));
        await progress({ task: task.name, attempt, pct: i * 10 });
      }
      const result = task.run(results);
      await world.events.create(runId, {
        eventType: 'step_completed',
        correlationId: id,
        eventData: { result },
      } as never);
      hooks.onStep?.(undefined);
      results[task.name] = result;
      continue;
    }

    if (task.kind === 'sleep') {
      if (has('wait_completed')) continue;
      const created = has('wait_created');
      if (!created) {
        const resumeAt = new Date(Date.now() + task.seconds * 1000);
        await world.events.create(runId, {
          eventType: 'wait_created',
          correlationId: id,
          eventData: { resumeAt },
        } as never);
        await progress({ task: task.name, note: `sleeping until ${resumeAt.toLocaleTimeString()}` });
        hooks.log(`sleeping ${task.seconds}s; the queue will redeliver at ${resumeAt.toLocaleTimeString()}`);
        return { timeoutSeconds: task.seconds };
      }
      const remaining = new Date(created.eventData.resumeAt).getTime() - Date.now();
      if (remaining > 0) return { timeoutSeconds: remaining / 1000 };
      await world.events.create(runId, { eventType: 'wait_completed', correlationId: id } as never);
      await progress({ task: task.name, note: 'woke up' });
      continue;
    }

    // hook
    const received = has('hook_received');
    if (received) {
      results[task.name] = received.eventData?.payload;
      if (!has('hook_disposed')) {
        await world.events.create(runId, { eventType: 'hook_disposed', correlationId: id } as never);
        await progress({ task: task.name, note: 'approved' });
      }
      continue;
    }
    if (!has('hook_created')) {
      await world.events.create(runId, {
        eventType: 'hook_created',
        correlationId: id,
        eventData: { token: hookToken(runId), metadata: { label: task.label } },
      } as never);
      await progress({ task: task.name, note: 'waiting for approval' });
    }
    hooks.log(`run …${short} is waiting on its hook; delivery ends idle`);
    return;
  }

  await world.events.create(runId, { eventType: 'run_completed', eventData: { output: results } } as never);
  await progress({ task: 'run', note: 'completed' });
  await world.streams.close(runId, stream);
  hooks.log(`run …${short} completed`);
  return;
}
