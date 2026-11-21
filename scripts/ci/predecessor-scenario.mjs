import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { promisify } from 'node:util';
import { property, source, startFixture } from './predecessor-fixture.mjs';

const exec = promisify(execFile);
const [scratch, name] = process.argv.slice(2);
const base = 'http://127.0.0.1:18133';
const fixtureDir = scratch + '/fixture';
const read = async file => (await readFile(scratch + '/' + file, 'utf8')).trim();
const docker = async (...args) => (await exec('docker', args, { timeout: 50000 })).stdout.trim();
const sql = async query => (await exec('psql', ['-XqAt', '-v', 'ON_ERROR_STOP=1', '-c', query], {
  timeout: 8000, env: { ...process.env, PGOPTIONS: '-c default_transaction_read_only=on -c search_path=ouf_ingestion,public' },
})).stdout.trim();

assert.equal(await sql("select current_setting('server_version_num')::int / 10000"), '17');
assert.equal(await sql("select to_regnamespace('ouf_ingestion') is null"), 't', 'expected empty module database');
const fixture = await startFixture(fixtureDir);
let running;

async function waitFor(label, condition) {
  console.log(label);
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    if (fixture.error) throw fixture.error;
    assert.equal(await docker('inspect', '-f', '{{.State.Status}}', running), 'running', label);
    const result = await condition();
    if (result) return result;
    await sleep(500);
  }
  throw new Error('Timed out: ' + label);
}

async function start(role, restart = false) {
  running = name + '-' + role;
  const image = await read((role === 'next' ? 'new' : role) + '.id');
  if (restart) {
    await docker('start', running);
  } else {
    const { PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD } = process.env;
    await docker('run', '-d', '--name', running, '--network', 'host', '-v', fixtureDir + ':/fixture:ro',
      '-e', `OUF_ING_DB_URL=jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}`,
      '-e', 'OUF_ING_DB_USER=' + PGUSER, '-e', 'OUF_ING_DB_PASSWORD=' + PGPASSWORD, image,
      '--server.address=127.0.0.1', '--server.port=18133', '--spring.flyway.enabled=true',
      '--spring.flyway.ignore-migration-patterns=*:future',
      ...(role === 'next' ? ['--spring.flyway.locations=classpath:db/migration,filesystem:/fixture/next-migration'] : []),
      ...fixture.args);
  }
  assert.equal(await docker('inspect', '-f', '{{.Image}}', running), image);
  await waitFor(`Readiness of ${role}${restart ? ' after return' : ''}`, async () => {
    let response;
    try { response = await fetch(base + '/actuator/health/readiness', { signal: AbortSignal.timeout(2000) }); }
    catch (error) {
      if (error.cause?.code === 'ECONNREFUSED' || error.name === 'TimeoutError') return false;
      throw error;
    }
    if (response.status === 503) return false;
    assert.equal(response.status, 200);
    const health = await response.json();
    if (health.status !== 'UP') return false;
    if (role === 'new') {
      const details = health.components.ingestionReadiness.details;
      assert.equal(details.schemaCompatibility, restart ? 'FUTURE_MIGRATIONS_PRESENT' : 'EXACT');
      assert.equal(details.futureMigrations, restart ? 1 : 0);
    }
    return true;
  });
}

async function history(role, extraScripts = []) {
  const rows = JSON.parse(await sql(`select json_agg(h order by installed_rank) from flyway_schema_history h`));
  assert.ok(rows.every(row => row.success), 'failed migration');
  const scripts = rows.filter(row => row.type === 'SQL').map(row => row.script).sort();
  const expected = (await read(role + '.migrations')).split('\n').concat(extraScripts).sort();
  assert.deepEqual(scripts, expected, role + ' migration history');
  console.log(`${role} schema V${rows.at(-1).version}, ${scripts.length} SQL migrations`);
  return rows;
}

async function record(publication, expected) {
  // The only interpolated SQL value is the publication UUID generated in the fixture.
  const query = `select row_to_json(record) from (
    select r.run_id, r.state as run_state, p.state as partition_state, h.handoff_id,
           h.idempotency_key, h.payload_json, h.attempts, h.state, h.last_error_code,
           h.downstream_receipt_ref, w.committed_handoff_id
    from ing_run r left join ing_partition p using(run_id)
    left join handoff_outbox h using(run_id,partition_key)
    left join ing_watermark w on w.source_id=r.source_id and w.partition_key=p.partition_key
    where r.publication_id='${publication}'
  ) record`;
  return waitFor('Outbox ' + expected + ' for ' + publication, async () => {
    const row = await sql(query);
    if (!row) return false;
    const result = JSON.parse(row); // Multiple rows for the single CSV record also fail.
    assert.ok(!['FAILED', 'PAUSED', 'ABORTED'].includes(result.run_state), result.run_state);
    if (result.state !== expected) return false;
    if (expected === 'ACKED' && result.run_state !== 'SUCCEEDED') return false;
    return result;
  });
}

function verifyAck(row, attempts, value) {
  assert.equal(row.partition_state, 'SUCCEEDED');
  assert.equal(row.attempts, attempts);
  assert.equal(row.payload_json.canonicalPayload[property], value);
  assert.equal(row.payload_json.ingestionRunId, row.run_id);
  assert.equal(row.payload_json.handoffId, row.handoff_id);
  assert.equal(row.downstream_receipt_ref, 'udp://handoffs/' + row.handoff_id);
  assert.equal(row.committed_handoff_id, row.handoff_id);
}

async function verifyRead(operation, runs) {
  const items = await fixture.read(base, operation);
  assert.deepEqual(items.map(r => [r.source_ref, r.publication_id, r.job_ref, r.run_state]),
    runs.map(([publication, row]) => [source, publication, row.run_id, 'SUCCEEDED']), operation);
}

try {
  await start('old');
  const oldHistory = await history('old');
  const first = fixture.publish(1, 'Library');
  const baseline = await record(first, 'ACKED');
  verifyAck(baseline, 1, 'Library');
  await docker('stop', '-t', '35', running);

  await start('new');
  const upgradedHistory = await history('new');
  assert.deepEqual(upgradedHistory.slice(0, oldHistory.length), oldHistory, 'predecessor history changed');
  await verifyRead('status', [[first, baseline]]);
  fixture.holdAck = true;
  const second = fixture.publish(2, 'Station');
  const pending = await record(second, 'FAILED_RETRYABLE');
  assert.equal(pending.last_error_code, 'ING_EXECUTION_GATEWAY_503');
  assert.equal(pending.attempts, 1);
  assert.equal(pending.committed_handoff_id, baseline.handoff_id, 'watermark advanced without ACK');
  assert.ok(pending.idempotency_key);
  await docker('stop', '-t', '35', running);
  assert.deepEqual(fixture.deliveries, [baseline.payload_json, pending.payload_json]);

  fixture.holdAck = false;
  await start('old', true);
  assert.deepEqual(await history('new'), upgradedHistory, 'Flyway history changed on return');
  const retried = await record(second, 'ACKED');
  verifyAck(retried, 2, 'Station');
  assert.deepEqual(retried.payload_json, pending.payload_json);
  assert.equal(retried.idempotency_key, pending.idempotency_key);
  await verifyRead('status', [[second, retried]]);
  await verifyRead('history', [[second, retried], [first, baseline]]);

  const third = fixture.publish(3, 'Museum');
  const afterReturn = await record(third, 'ACKED');
  verifyAck(afterReturn, 1, 'Museum');
  await verifyRead('history', [[third, afterReturn], [second, retried], [first, baseline]]);
  assert.deepEqual(fixture.deliveries, [baseline.payload_json, pending.payload_json, pending.payload_json, afterReturn.payload_json]);
  assert.equal(await sql(`select (select count(*) from ing_run), (select count(*) from ing_partition),
                                (select count(*) from handoff_outbox)`), '3|3|3');
  console.log('PASS: three managed runs, predecessor reads, same candidate handoff retried and acknowledged.');

  await docker('stop', '-t', '35', running);
  const nextVersion = upgradedHistory.at(-1).version + '.1';
  assert.match(nextVersion, /^\d+(?:\.\d+)+$/);
  const probe = 'V' + nextVersion + '__ci_predecessor_return_probe.sql';
  await mkdir(fixtureDir + '/next-migration');
  await writeFile(fixtureDir + '/next-migration/' + probe,
    'CREATE TABLE ouf_ingestion.ci_predecessor_return_probe (id integer PRIMARY KEY);\n');
  await start('next');
  const nextHistory = await history('new', [probe]);
  assert.deepEqual(nextHistory.slice(0, -1), upgradedHistory, 'synthetic release changed the candidate prefix');
  assert.equal(nextHistory.at(-1).version, nextVersion);
  assert.equal(await sql("select to_regclass('ouf_ingestion.ci_predecessor_return_probe') is not null"), 't');
  await docker('stop', '-t', '35', running);
  await start('new', true);
  assert.deepEqual(await history('new', [probe]), nextHistory, 'history changed after candidate restart');
  console.log('PASS: candidate ready after next-release migration (FUTURE_MIGRATIONS_PRESENT, 1).');
} catch (error) {
  // Keep failure evidence to schema versions, states and owner error codes.
  for (const query of [
    'select version, success from flyway_schema_history where version is not null order by installed_rank desc limit 1',
    'select state, failure_code from ing_run order by created_at limit 10',
    'select state, attempts, last_error_code from handoff_outbox order by created_at limit 10',
  ]) {
    const facts = await sql(query).catch(() => 'unavailable');
    console.error(query + '\n' + facts);
  }
  throw error;
} finally {
  fixture.close();
}
