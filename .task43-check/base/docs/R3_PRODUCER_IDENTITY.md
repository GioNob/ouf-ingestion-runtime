# Ingestion: ricevuta operativa APISIX

Con `ouf.summary.enabled=true`, una catena Spring Security dedicata (ordine -1)
copre esattamente questi URI, per tutti i metodi; il filtro ammette solo POST:

- `/api/internal/v1/ingestion/operations/summary`
- `/api/internal/v1/ingestion/operations/incidents`

APISIX inoltra i soli Arguments come body JSON e firma
`X-OUF-Operational-Receipt`. Non si usano gli header d'identità `X-OUF-*`, cookie
o principal preesistenti per autenticare questa richiesta. Qualsiasi header
`Authorization`, anche vuoto, causa rifiuto; non esiste fallback JWT sui due URI
quando la catena è attiva. La ricevuta status/history
`X-OUF-Ingestion-Read-Receipt` resta un contratto separato. IAM, status/history,
explain, sottopercorsi e altre route non cambiano. Proprietà assente o false:
nessuna nuova catena, comportamento IAM precedente.

## Configurazione compatibile con 3d8dd34

| Proprietà | Significato |
|---|---|
| `ouf.summary.enabled` | Nuovo opt-in, false per default; il filtro storico era incondizionato. |
| `ouf.summary.receipt-key-file` | File della chiave del produttore, 64 caratteri esadecimali usati come byte ASCII; spazi esterni rimossi come nella versione storica. |
| `ouf.summary.tenant-id` | Tenant atteso. |
| `ouf.summary.issuer` | Issuer atteso. |
| `ouf.summary.audience` | Audience attesa. |
| `ouf.summary.workload` | Workload MCP atteso. |

I cinque nomi e i significati storici sono invariati; nessun valore
d'installazione è incorporato nel codice. I mount/configurazioni esistenti possono
essere riutilizzati aggiungendo l'opt-in; questo task non li legge o modifica.
La chiave viene riletta per ogni richiesta, come in `3d8dd34`.
Restano necessarie le configurazioni `ouf.authorization.*` del bundle locale.

## Contratto e autorizzazione owner

Formato senza padding: `base64url(json).base64url(mac)`, con HMAC-SHA256 su
`ouf-operational-owner-v1.` seguito dal primo segmento. Il filtro verifica
`v=1`, `purpose=operational-summary-owner` (anche incidents), metodo POST,
path/capability esatti, tenant/issuer/audience/workload configurati e SHA-256
esadecimale minuscolo dei byte del body effettivamente letti. Lo stesso body è
riproposto al controller. Limiti ereditati: body 65536 byte, ricevuta 16384
caratteri, testi 4096 caratteri senza controlli, massimo 32 ruoli. `roles` può
essere la stringa vuota; gli altri testi richiesti non possono essere vuoti.
JSON duplicato/trailing, numeri non interi/fuori long e base64url non canonico
sono rifiutati. Tempi: `iat <= now < exp`, durata positiva al massimo 30 secondi,
senza tolleranza aggiuntiva e senza overflow.

La ricevuta costruisce un `TrustedPrincipal` HUMAN con subject, tenant, client,
ACR, issuer, audience, scope e ruoli firmati. Il client occupa il campo SDK
`servicePrincipalId`, come nel filtro storico; non è il workload MCP. AMR e
istante di autenticazione non vengono inventati.

Prima del controller, lo stesso SDK locale richiede:

| Operazione | Capability produttore | Scope |
|---|---|---|
| summary | `ouf.ingestion.operations.summary` | `operations.status.read` |
| incidents | `ouf.ingestion.operations.incidents` | `operations.incident.read` |

La risorsa è `capability`, dettaglio `TENANT_OPERATIONAL`. Il `decisionRef`
della decisione locale deve coincidere esattamente con quello firmato:
`bundleId:version:capabilityId`. Il controller usa poi il medesimo principal e
snapshot SDK tramite `authorization.owner(...)`; i controlli già esistenti in
`service.summary`/`service.incidents` restano sul dettaglio `TENANT_OPERATIONAL`,
risorsa `operational`, modulo INGESTION, con vincoli tenant/source/job. Servono
anche i relativi grant operativi: un grant sul solo capability produttore non
rilascia i risultati. Le risposte negate/redatte non diventano HEALTHY.

## Risposte ed evidenza

- 403 `INVALID_OPERATIONAL_RECEIPT`: ricevuta assente, duplicata o invalida,
  binding/body/tempi/scope errati, oppure presenza di Authorization.
- 503 `OWNER_IDENTITY_UNAVAILABLE`: binding d'identità assenti/invalidi o file
  chiave assente, illeggibile, malformato. L'applicazione rimane avviabile.
- 403 `NOT_AUTHORIZED`: policy locale assente, scaduta, senza grant/dettaglio
  o decisionRef divergente. Nessun retry automatico degli errori d'identità.
- 405: metodo diverso da POST. 413: body oltre limite.

I codici usano `sendError` come nel filtro storico: sono reason servlet, non un
nuovo formato JSON di risposta. Nessun log contiene chiavi, body o ricevute.

I vettori positivi sono copie immutate dei vettori Lua del gateway:
`81fdf7a:operational-owner/src/test/resources/operational-receipts.json` e
`24eba09:operational-owner/src/test/resources/incident-receipts.json`.
La chiave sintetica pubblica di test è `ab` ripetuto 32 volte. Non si rigenerano
i vettori positivi con il verificatore Java; il signer nei test serve a
mutazioni/casi negativi. I test HTTP usano controller e serializzazione owner
reali con accesso dati simulato, più JWT decoder simulato per verificare la
selezione delle catene. Non costituiscono evidenza end-to-end APISIX/VPS.

Un controllo offline indipendente con Node ha verificato tutti i sei vettori:
copia identica ai file storici, HMAC con chiave ASCII e SHA-256 del body corretti.
Questo controllo non esegue il filtro Java.

Verifica prevista: `mvn -o -q test` con JDK 21 e dipendenze già disponibili;
senza vincolo offline, la CI può usare `mvn -q test`. Nell'ambiente di questo
task Maven non è disponibile e la suite non è stata eseguita. Restano alla CI
la compilazione e i test completi; al deployment la verifica dei mount, opt-in,
grant ACTIVE, compatibilità APISIX e confine di rete. Nessun accesso al VPS,
segreti, pubblicazione di policy o modifica di altri repository è stato fatto.
