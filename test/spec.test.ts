import { createTestSuite } from '@workflow/world-testing';

// The Workflow SDK's World compatibility suite: it spawns a server with
// WORKFLOW_TARGET_WORLD pointing at the built package and runs real
// workflows through the core runtime against it. In Node the World uses an
// in-memory IndexedDB (fake-indexeddb), which is enough because the suite's
// server is a single process.
createTestSuite('./dist/index.js');
