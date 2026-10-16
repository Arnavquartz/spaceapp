// Export real Jaeger storage data without modifying the returned trace JSON.
const { readFile, writeFile } = require('node:fs/promises');
const { resolve, join } = require('node:path');
const assert = require('node:assert/strict');

async function main() {
  const output = resolve(process.argv[2]);
  const run = JSON.parse(await readFile(join(output, 'run.json'), 'utf8'));
  assert.match(run.traceId, /^[a-f0-9]{32}$/);
  const response = await fetch(`http://127.0.0.1:16686/api/traces/${run.traceId}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  assert.equal(response.status, 200);
  const data = JSON.parse(bytes.toString('utf8'));
  assert.equal(data.data.length, 1);
  const trace = data.data[0];
  assert.equal(trace.traceID, run.traceId);
  assert.equal(trace.spans.length, run.expectedSpans);
  const tag = (span, key) => span.tags.find(item => item.key === key)?.value;
  const api = trace.spans.find(span => trace.processes[span.processID].serviceName === 'space-api');
  const workflow = trace.spans.find(span => tag(span, 'workflowType') && !tag(span, 'activityType'));
  const activity = trace.spans.find(span => tag(span, 'activityType'));
  assert.ok(api && workflow && activity);
  assert.equal(api.spanID, run.apiSpanId);
  const parentOf = span => span.references.find(ref => ref.refType === 'CHILD_OF')?.spanID;
  assert.equal(parentOf(workflow), api.spanID);
  assert.equal(parentOf(activity), workflow.spanID);
  assert.ok(trace.spans.every(span => span.traceID === run.traceId));
  const raw = bytes.toString('utf8');
  assert.ok(!raw.includes('Synthetic private prompt'));
  assert.ok(!raw.includes('Synthetic provider response'));
  await writeFile(join(output, 'jaeger-trace.json'), bytes);
  const verification = {
    traceId: run.traceId,
    spans: trace.spans.map(span => ({
      name: span.operationName,
      spanId: span.spanID,
      parentSpanId: parentOf(span) ?? null,
      service: trace.processes[span.processID].serviceName,
      durationMicroseconds: span.duration
    })),
    connectedParentChain: true,
    privatePromptAndProviderOutputExcluded: true,
    bytes: bytes.length,
    provenance: 'Unmodified response from local Jaeger query storage, not a constructed trace.'
  };
  await writeFile(join(output, 'trace-verification.json'), JSON.stringify(verification, null, 2) + '\n');
  console.log(JSON.stringify(verification, null, 2));
}

main().catch(error => { console.error(error); process.exitCode = 1; });
