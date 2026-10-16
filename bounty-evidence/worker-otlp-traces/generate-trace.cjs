/* Real loopback HTTP request, shipping SpaceApp instrumentation and workflow,
 * stub provider activity, real Temporal server, and real Jaeger collector.
 * Usage: node generate-trace.cjs /absolute/path/to/spaceapp /absolute/output/dir
 * Build the repository first. Jaeger must accept OTLP at 127.0.0.1:4318.
 */
const { createRequire } = require('node:module');
const { pathToFileURL } = require('node:url');
const { resolve, join } = require('node:path');
const { mkdir, writeFile } = require('node:fs/promises');
const { randomUUID } = require('node:crypto');
const assert = require('node:assert/strict');

async function main() {
  const repo = resolve(process.argv[2]);
  const output = resolve(process.argv[3]);
  const fromRepo = createRequire(join(repo, 'package.json'));
  const Fastify = fromRepo('fastify');
  const { TestWorkflowEnvironment } = fromRepo('@temporalio/testing');
  const { Worker, bundleWorkflowCode } = fromRepo('@temporalio/worker');
  const load = path => import(pathToFileURL(join(repo, path)).href);
  const { dummyTurnInputSchema } = await load('packages/contracts/dist/index.js');
  const [httpModule, contextModule, exporterModule, workerModule] = await Promise.all([
    load('apps/api/dist/observability.js'),
    load('apps/api/dist/trace-context.js'),
    load('packages/runtime/dist/index.js'),
    load('apps/worker/dist/worker-tracing.js')
  ]);
  const endpoint = 'http://127.0.0.1:4318';
  const apiExporter = exporterModule.createOtlpSpanExporter({ endpoint, serviceName: 'space-api' });
  const httpTracing = httpModule.createHttpObservability({ serviceName: 'space-api', spanExporter: apiExporter });
  const registry = new contextModule.TraceContextRegistry();
  const workerTracing = workerModule.createWorkerTracing({
    serviceName: 'space-worker',
    env: { SPACE_OTEL_EXPORTER_OTLP_ENDPOINT: endpoint }
  });
  const api = Fastify({ genReqId: () => `req:jaeger-${randomUUID()}` });
  api.addHook('onRequest', async request => httpTracing.onRequest(request));
  api.addHook('onResponse', async (request, reply) => httpTracing.onResponse(request, reply));
  const environment = await TestWorkflowEnvironment.createTimeSkipping();
  const taskQueue = `jaeger-proof-${randomUUID()}`;
  const workflowId = `codex-jaeger-proof-${randomUUID()}`;
  const bundle = await bundleWorkflowCode({
    workflowsPath: join(repo, 'apps/worker/dist/workflows.js'),
    workflowInterceptorModules: workerTracing.interceptors.workflowModules
  });
  const { workflowModules: _bundled, ...hostInterceptors } = workerTracing.interceptors;
  let calls = 0;
  let parent;
  const worker = await Worker.create({
    connection: environment.nativeConnection,
    taskQueue,
    workflowBundle: bundle,
    interceptors: hostInterceptors,
    sinks: workerTracing.sinks,
    shutdownGraceTime: '2s',
    activities: {
      async runCodexAppServerTurn(input) {
        calls += 1;
        return {
          workflowId,
          roomId: input.roomId,
          paneId: input.paneId,
          traceId: input.traceId,
          status: 'COMPLETED',
          message: 'Synthetic provider response; no model was contacted.'
        };
      }
    }
  });
  api.post('/bounty-proof/turn', async request => {
    registry.remember(request.id, contextModule.traceContextForRawRequest(request.raw));
    const traceparent = contextModule.traceparentForTraceId(request.id, registry);
    assert.ok(traceparent);
    const [, traceId, spanId] = traceparent.split('-');
    parent = { traceId, spanId, traceparent };
    const input = dummyTurnInputSchema.parse({
      roomId: 'room:jaeger-proof',
      paneId: 'pane:jaeger-proof',
      prompt: 'Synthetic private prompt that must not appear in worker span attributes.',
      traceId: request.id,
      providerId: 'codex',
      artifactIds: [],
      permissionMode: null
    });
    return environment.client.workflow.execute('codexAppServerTurnWorkflow', {
      taskQueue,
      workflowId,
      workflowExecutionTimeout: '30s',
      args: [input],
      memo: { traceparent, spaceTraceId: request.id }
    });
  });
  let responseBody;
  try {
    const address = await api.listen({ host: '127.0.0.1', port: 0 });
    await worker.runUntil(async () => {
      const response = await fetch(`${address}/bounty-proof/turn`, { method: 'POST' });
      responseBody = await response.json();
      assert.equal(response.status, 200, JSON.stringify(responseBody));
      assert.equal(responseBody.status, 'COMPLETED');
    });
  } finally {
    await api.close();
    httpTracing.close();
    await Promise.all([apiExporter.close(), workerTracing.close()]);
    await environment.teardown();
  }
  assert.equal(calls, 1);
  assert.ok(parent);
  const apiStats = apiExporter.stats();
  const workerStats = workerTracing.stats();
  assert.equal(apiStats.exported, 1);
  assert.equal(workerStats.exported, 2);
  assert.equal(apiStats.failed + workerStats.failed, 0);
  const result = {
    traceId: parent.traceId,
    apiSpanId: parent.spanId,
    workflowId,
    workflowType: 'codexAppServerTurnWorkflow',
    expectedSpans: 3,
    apiStats,
    workerStats,
    request: { method: 'POST', path: '/bounty-proof/turn', transport: 'real loopback HTTP' },
    collector: 'Jaeger 2.22.0 OTLP/HTTP at 127.0.0.1:4318',
    execution: 'Shipping compiled API instrumentation and workflow; one real Temporal Worker with a stub provider activity; real time-skipping Temporal server.',
    limitations: 'Synthetic route and provider activity; this is not a live provider run or a production deployment.'
  };
  await mkdir(output, { recursive: true });
  await writeFile(join(output, 'run.json'), JSON.stringify(result, null, 2) + '\n');
  console.log(JSON.stringify(result, null, 2));
}

main().catch(error => { console.error(error); process.exitCode = 1; });
