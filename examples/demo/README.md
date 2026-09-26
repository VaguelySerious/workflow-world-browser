# Demo: a durable workflow in one browser tab

**Live:** https://pranaygp.github.io/workflow-world-browser/

A six-stage order workflow (reserve inventory, charge card, an 8 second
durable sleep, a hook that waits for you to press **Approve**, ship, send a
receipt; each step takes about 2 seconds) runs on `workflow-world-browser` with no server. The page shows the
run's live progress stream, its raw event log (slot, `eventType`,
`correlationId`), the queue rows in IndexedDB, and which tab holds the Web
Lock.

## What runs the workflow

**This demo does not run the Workflow SDK runtime.** It runs a small durable
runner written directly against the World interface
([`src/runner.ts`](src/runner.ts), about 250 lines). The World underneath is
the same one the SDK uses, and it passes the SDK's World compatibility suite
(`pnpm test` at the repo root), but the SDK runtime itself does not run in a
browser today:

- Workflow code has to be compiled by the SDK's SWC plugin into separate
  workflow and step bundles. That happens in a server build (Next.js, Vite,
  Nitro, ...), not in a page.
- The runtime replays workflow code in `node:vm` (or a QuickJS WASI build
  meant for server runtimes), and imports `node:assert`, `node:util` and
  `node:async_hooks`.

So the runner speaks the World protocol by hand, the same protocol the runtime
uses on any World:

| Workflow operation | What the runner writes |
| --- | --- |
| start | `run_created`, then a queue message; the first delivery writes `run_started` |
| step | a born-running `step_started` (the World adds a synthetic `step_created`), then `step_completed` with the result |
| sleep | `wait_created` with `resumeAt`; the delivery returns `{ timeoutSeconds }` so the queue redelivers at `resumeAt`; then `wait_completed` |
| hook | `hook_created` with a token; the delivery ends idle. **Approve** writes `hook_received` (deduplicated by `resumeId`) and enqueues a wake-up |
| finish | `run_completed` with every step's result |

Every delivery replays the run from its event log. A step whose
`step_completed` is in the log is never run again, and later steps read its
result from there (the receipt names the same `chargeId` even if the tab died
in between). A `step_started` with no `step_completed` after it means the tab
running that step died, so the next delivery runs it again as attempt 2.

## Try it: recovery after a crash

1. Open the [live demo](https://pranaygp.github.io/workflow-world-browser/)
   (or run it locally, below) and press **Start a run**.
2. While a step is running (for example **Charge card**, blue dot), press
   **Crash this tab**. It reloads the page on the spot: no cleanup runs, and
   the in-flight queue message stays in IndexedDB, because the queue deletes a
   message only after its delivery returns.
3. The reloaded tab re-acquires the Web Lock, redelivers that message, and the
   step shows `attempt 2 · recovered after a crash`. In the event log you will
   see two `step_started` events for that step and one `step_completed`.
4. Crash it again during the sleep: the sleep keeps counting down from its
   stored `resumeAt`, and the queue row shows when it is due.
5. When the run reaches **Wait for approval**, press **Approve**. The run
   finishes and its status turns `completed`.

You can also reload with the browser's reload button, or close the tab and
reopen the page: the run picks up where it stopped.

## Try it: two tabs, one driver

1. With the demo open, press **Open a second tab**. The new tab says
   **Standby**: another tab holds the Web Lock. The Web Locks panel shows the
   driver lock held with one tab waiting.
2. Press **Start a run** in the standby tab. It writes `run_created` and the
   queue row; the driving tab picks the message up (see its "This tab"
   activity) and executes the steps, while both tabs show the same live
   progress (streams are IndexedDB rows with `BroadcastChannel` hints).
3. While a step is running, close the driving tab. The standby tab turns
   **Driving**, redelivers the in-flight message, and runs the interrupted step
   again as attempt 2.
4. **Approve** works from either tab.

How the lock works: only the tab holding a demo-level Web Lock registers the
queue handler. Registering it makes the World request its own consumer lock
(`workflow-world-browser:<db>:queue`) and start delivering. When the tab goes
away, the browser releases both locks and the next tab in line takes over. The
World does not need the demo-level lock; it is there so the page can tell you
which role it has.

## Run it locally

From the repository root:

```bash
pnpm install
pnpm build                                    # builds the World into dist/
pnpm --filter workflow-world-browser-demo dev # http://localhost:5173
```

Data lives in the `wwb-demo` IndexedDB database of that origin. **Clear all
data** empties it.

## Not shown

Retries of a failing step (`step_retrying`, `retryAfter`) and step
concurrency. The World supports both; the runner keeps to a straight line so
the event log stays readable.
