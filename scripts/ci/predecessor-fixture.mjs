// Local peers for the managed CSV scenario in ManagedRepeatabilityRuntimeTest.
import assert from 'node:assert/strict';
import { createHash, createHmac, randomBytes, randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';

export const source = 'source-a';
export const property = 'https://example.test/name';
export const latitudeProperty = 'https://example.test/latitude';
export const geometryProperty = 'https://example.test/geometry';
export const conceptProperty = 'https://example.test/place-type';
export const coordinates = ['13.77012345678901234567', '45.65012345678901234567'];
export const concept = 'https://example.test/concept/station';
const tenant = 'tenant-a';
const scope = 'ingestion.operations.read';
const semantic = { semanticId: 'core', semanticVersion: '1', revisionId: randomUUID(), publicationSetId: randomUUID() };
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const issuer = 'https://auth.test/realms/ouf';
const sorted = (_, v) => v && typeof v === 'object' && !Array.isArray(v)
  ? Object.fromEntries(Object.keys(v).sort().map(k => [k, v[k]])) : v;
const targetVocabulary = 'https://example.test/vocabulary/place-type@' + randomUUID();
const definition = { targetVocabulary, entries: [{ code: 'ST', concept }] };
const map = {
  semanticId: 'https://example.test/value-map/place-type', semanticVersion: '1',
  revisionId: randomUUID(), publicationSetId: semantic.publicationSetId,
  targetVocabulary, definition, definitionSha256: digest(JSON.stringify(definition, sorted)),
};
map.valueMapRef = map.semanticId + '@' + map.publicationSetId;

function publication(sequence, value, latitude) {
  const columns = ['id', 'name'];
  const cells = [sequence, value];
  const mappings = [{ sourceField: 'name', targetPropertyIri: property, transform: 'IDENTITY' }];
  const bindings = [semantic];
  if (latitude !== undefined) {
    columns.push('latitude', 'geometry', 'placeTypeCode');
    cells.push(latitude, `POINT (${coordinates.join(' ')})`, 'ST');
    mappings.push({ sourceField: 'latitude', targetPropertyIri: latitudeProperty, transform: 'DECIMAL_DOT_V1' });
    mappings.push({ sourceField: 'geometry', targetPropertyIri: geometryProperty, transform: 'WKT_V1',
      transformParams: { crs: 'EPSG:4326' } });
    mappings.push({ sourceField: 'placeTypeCode', targetPropertyIri: conceptProperty, transform: 'VALUE_MAP_V1',
      transformParams: { valueMapRef: map.valueMapRef } });
    bindings.push(map);
  }
  const csv = Buffer.from(`${columns.join(';')}\n${cells.join(';')}\n`);
  const ref = `object://files/approved-${sequence}.csv`;
  const execution = {
    acquisitionMode: 'INTERNAL_MANAGED_CSV', adapterId: 'managed-tabular-v1', adapterRuntimeVersion: '1.0.0',
    sourceSchemaRef: 'schema:1', sourceSchemaId: 'places', sourceSchemaVersion: '1',
    semanticPublicationSetRef: 'semantic:1', adapterProfileRef: 'adapter:1', observationPolicy: 'ACQUISITION_TIME',
    expectedSize: csv.length, maxRows: 100, maxColumns: 20, maxCellChars: 1000,
  };
  const bundle = {
    bundleId: 'bundle-' + source, bundleVersion: String(sequence), status: 'ACTIVE',
    source: { sourceId: source, sourceKind: 'INTERNAL_MANAGED', acquisitionMode: 'MANAGED' },
    createdFromOnboardingVersion: randomUUID(), syncProfile: {},
    semanticRefs: bindings.map(pin => pin.semanticId + '@' + pin.semanticVersion), semanticReferenceBindings: bindings,
    extractionProfile: { runtime: { execution, stagingRef: ref, contentHash: 'sha256:' + digest(csv), csvDelimiter: ';' },
      projection: { PLACE: columns } },
    semanticMapping: { sourceType: { sourceId: source, typeCode: 'PLACE' },
      propertyMappings: mappings },
    sourceObjectIdentityPolicy: { strategy: 'NATIVE_KEY', sourceFields: ['id'] },
    changeRepresentationProfile: { mode: 'FULL_SNAPSHOT' },
    dataAccessPolicies: mappings.map(m => ({ target: m.targetPropertyIri, label: 'OPEN', scope: 'PROPERTY' })),
  };
  // PublishedActivation hashes sorted map keys, preserving array order.
  bundle.checksum = 'sha256:' + digest(JSON.stringify(bundle, sorted));
  return { csv, ref, envelope: { publicationId: randomUUID(), publicationSequence: sequence, tenantId: tenant,
    sourceId: source, sourceStatus: 'ENABLED', onboardingVersionId: bundle.createdFromOnboardingVersion,
    checksum: bundle.checksum, bundle } };
}

export async function startFixture(dir) {
  const token = randomBytes(32).toString('hex');
  const key = randomBytes(32).toString('hex');
  const policy = JSON.stringify({
    bundleId: 'fixture', version: 1, publishedAt: new Date().toISOString(),
    capabilities: [{ capabilityId: scope, operation: 'EXECUTE', requiredScope: scope, allowedActors: ['HUMAN'] }],
    grants: [{ grantId: 'grant:read', capabilityId: scope, subjectId: 'reader', tenantId: tenant,
      validFrom: new Date(Date.now() - 60000).toISOString(), validUntil: new Date(Date.now() + 3600000).toISOString(),
      constraints: { resourceType: 'operational', resourceAttributes: { sourceRef: source },
        allowedDetailLevels: ['TENANT_OPERATIONAL'] } }],
  });
  await mkdir(dir);
  for (const [file, data] of Object.entries({ token, key, policy })) {
    await writeFile(`${dir}/${file}`, data, { mode: 0o444 });
  }
  let current;
  const fixture = {
    deliveries: [], deliveryBodies: [], rawObjects: new Map(), holdAck: false,
    publish(sequence, value, latitude) {
      current = publication(sequence, value, latitude);
      return current.envelope.publicationId;
    },
  };
  const server = createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    function reply(status, data) {
      res.writeHead(status, { 'Content-Type': Buffer.isBuffer(data) ? 'text/csv' : 'application/json' });
      res.end(Buffer.isBuffer(data) ? data : JSON.stringify(data));
    }
    try {
      if (req.headers.authorization !== 'Bearer ' + token) return reply(403, {});
      if (req.method === 'GET') {
        const publications = '/api/onboarding/v1/runtime/publications';
        if (url.pathname === publications) return reply(200, { items: current ? [current.envelope] : [], nextAfter: '' });
        if (current && url.pathname === publications + '/' + source + '/active') return reply(200, current.envelope);
        if (url.pathname === '/api/semantic/v1/references:resolve') {
          const pin = current?.envelope.bundle.semanticReferenceBindings.find(p => p.semanticId === url.searchParams.get('semanticId'));
          assert.ok(pin);
          for (const field of ['revisionId', 'publicationSetId']) assert.equal(url.searchParams.get(field), pin[field]);
          return reply(200, { semantic_id: pin.semanticId, semantic_version: pin.semanticVersion, revision_id: pin.revisionId,
            publication_set_id: pin.publicationSetId, status: 'ACTIVE', ...(pin === map && {
              artifact_type: 'VALUE_MAP', definition: pin.definition, definitionSha256: pin.definitionSha256,
            }) });
        }
        if (current && url.pathname === '/internal/object-storage/v1/content' && url.searchParams.get('ref') === current.ref)
          return reply(200, current.csv);
      }
      if (req.method === 'POST') {
        const chunks = [];
        for await (const chunk of req) chunks.push(chunk);
        const bodyText = Buffer.concat(chunks).toString('utf8');
        const body = JSON.parse(bodyText);
        if (url.pathname === '/api/internal/v1/lake/objects') {
          const content = Buffer.from(body.contentBase64, 'base64');
          const hash = 'sha256:' + digest(content);
          assert.equal(body.contentHash, hash);
          const objectRef = 'lake:' + body.idempotencyKey;
          if (body.zone === 'RAW') fixture.rawObjects.set(objectRef, JSON.parse(content));
          return reply(201, { durable: true, contentHash: hash, objectRef });
        }
        if (url.pathname === '/api/internal/v1/handoffs') {
          fixture.deliveries.push(body);
          fixture.deliveryBodies.push(bodyText);
          if (fixture.holdAck) {
            res.setHeader('Retry-After', '30');
            return reply(503, {});
          }
          return reply(201, { durable: true, receiptRef: 'udp://handoffs/' + body.handoffId });
        }
      }
      reply(404, {});
    } catch (error) {
      fixture.error = error;
      reply(500, {});
    }
  });
  await new Promise((resolve, reject) => server.once('error', reject).listen(0, '127.0.0.1', resolve));
  fixture.close = () => server.close();
  fixture.args = [
    '--ouf.ingestion.activation.enabled=true', '--ouf.ingestion.execution.enabled=true',
    '--ouf.ingestion.activation.gateway-url=http://127.0.0.1:' + server.address().port,
    '--ouf.ingestion.activation.token-file=/fixture/token', '--ouf.ingestion.activation.tenant-id=' + tenant,
    '--ouf.ingestion.activation.poll-ms=500', '--ouf.ingestion.execution.poll-delay-ms=500',
    '--ouf.ingestion.gateway-read.enabled=true', '--ouf.ingestion.gateway-read.issuer=' + issuer,
    '--ouf.ingestion.gateway-read.audience=gateway', '--ouf.ingestion.gateway-read.workload=workload',
    '--ouf.ingestion.gateway-read.key-file=/fixture/key', '--ouf.authorization.bundle-file=/fixture/policy',
    '--ouf.authorization.bundle-id=fixture', '--ouf.authorization.bundle-version=1',
    '--ouf.authorization.bundle-sha256=' + digest(policy),
  ];
  // Same signed receipt and owner-policy boundaries as GatewayReadReceiptVerifierTest
  // and OperationalOwnerAuthorizationTest. Each request binds its own body and audit headers.
  fixture.read = async (base, operation) => {
    const path = '/api/internal/v1/ingestion/operations/' + operation;
    const body = JSON.stringify({ sourceId: source, limit: 10 });
    const iat = Math.floor(Date.now() / 1000);
    const receipt = { v: 1, purpose: 'ingestion-read-owner', method: 'POST', path, bodyHash: digest(body),
      capability: 'ouf.ingestion.' + operation, iat, exp: iat + 30, issuer, audience: 'gateway', workload: 'workload',
      actor: 'HUMAN', subject: 'reader', tenant, acr: '1', roles: '', scope, idempotencyKey: randomUUID(),
      correlationId: randomUUID(), attemptId: randomUUID(), decisionRef: 'decision:fixture' };
    const encoded = Buffer.from(JSON.stringify(receipt)).toString('base64url');
    const signature = createHmac('sha256', key).update('ouf-ingestion-read-owner-v1.' + encoded).digest('base64url');
    const response = await fetch(base + path, { method: 'POST', body, signal: AbortSignal.timeout(5000), headers: {
      'Content-Type': 'application/json', 'X-OUF-Ingestion-Read-Receipt': encoded + '.' + signature,
      'X-Correlation-ID': receipt.correlationId, 'X-Tool-Attempt-ID': receipt.attemptId, 'Idempotency-Key': receipt.idempotencyKey,
    } });
    assert.equal(response.status, 200, operation);
    const page = await response.json();
    assert.equal(page.authorization, 'AUTHORIZED');
    assert.equal(page.partial, false);
    return page.items;
  };
  return fixture;
}
