# Ritorno al predecessore di Ingestion (D-080)

Il predecessore supportato per questo rilascio è `6f0504240b2653e5aa93729ae70cedc52228515d`, in esercizio dal 21 novembre 2026 con schema V21. Il candidato del task-27b conserva V21 e introduce la readiness che gli permetterà di operare come predecessore dopo una migrazione successiva. Il riferimento di CI è lo SHA completo nel job `predecessor-return` di [module-ci.yml](../.github/workflows/module-ci.yml).

## Regola per le migrazioni

Ogni rilascio deve permettere di riavviare il predecessore sul database già migrato e popolato dal candidato. Durante la fase *expand*, il candidato conserva colonne, vincoli, viste e significato dei valori che il predecessore legge o scrive. La fase *contract* e l'abilitazione dei nuovi stati avvengono in un rilascio successivo, quando il codice in esercizio può gestirli.

Ogni task con migrazioni dichiara il predecessore supportato, le sue dipendenze SQL, i nuovi stati persistiti e il rilascio che ne autorizza la scrittura. Il piano di rilascio deve rifiutare i salti di versione privi di prova. Una trasformazione irreversibile richiede un mandato distinto e approvazione HUMAN esplicita. Si dichiara `rollbackCompatible: true` solo dopo la prova riuscita per quella coppia di revisioni.

## Contratto di readiness

Sul PostgreSQL 17 del modulo, la readiness legge la cronologia in ordine di installazione, in una transazione di sola lettura. Ammette il marker iniziale di creazione dello schema Flyway. Ogni migrazione del rilascio deve occupare la posizione attesa, con la stessa versione, lo stesso nome di script e checksum, tipo `SQL` ed esito positivo.

| Cronologia dopo il prefisso atteso | Readiness | Dettagli dell’indicatore |
|---|---|---|
| Nessuna riga | `UP` | `schemaCompatibility=EXACT`, `futureMigrations=0` |
| Da 1 a 16 migrazioni SQL riuscite, ciascuna con versione Flyway superiore all’ultima attesa | `UP` | `schemaCompatibility=FUTURE_MIGRATIONS_PRESENT`, `futureMigrations` indica il conteggio |
| Prefisso incompleto o alterato, coda non conforme o più di 16 righe future | `DOWN` | `reason=ING_SCHEMA_COMPATIBILITY_FAILED` |

L’endpoint `/actuator/health/readiness` espone questi dettagli in `components.ingestionReadiness.details`. Il campo `schemaVersion` identifica l’ultima versione attesa dal binario. Il campo `sqlMigrations` conta tutte le righe SQL accettate, comprese quelle future.

## Prova CI

Il job gira su push e pull request. Nel lancio manuale il parametro `predecessor` accetta uno SHA Git. Il candidato è sempre il `HEAD` del checkout del job. Quando cambia la versione in esercizio, si aggiornano insieme il pin nel workflow e questa pagina.

1. La CI costruisce ogni immagine dall'archivio Git del suo commit con il Dockerfile di quella revisione. Verifica `org.opencontainers.image.revision` e registra SHA e image ID nel log.
2. Il predecessore parte su un PostgreSQL 17 dedicato e vuoto. Flyway installa le migrazioni. Il runtime elabora una pubblicazione CSV e porta run e partizione a `SUCCEEDED`, con handoff `ACKED` e watermark associato.
3. Il candidato parte sullo stesso database, applica le proprie migrazioni e legge il run precedente via API. Elabora una seconda pubblicazione. La fixture risponde HTTP 503 con `Retry-After` alla consegna. La CI attende `FAILED_RETRYABLE` persistito e controlla che il watermark punti ancora al primo handoff.
4. Dopo lo stop del candidato riparte lo stesso container predecessore, con Flyway attivo e `*:future`. La CI richiede readiness `UP` e la stessa cronologia Flyway lasciata dal candidato. Il predecessore deve ritentare la consegna con lo stesso payload, handoff ID e chiave persistita, ottenere l'ACK e avanzare il watermark al secondo handoff.
5. Le API autenticate `status` e `history` devono restituire gli ID delle pubblicazioni e dei run attesi. Una terza pubblicazione deve completarsi sul predecessore. Il totale finale è di tre run, tre partizioni e tre righe di outbox.
6. La CI ferma il predecessore e simula il rilascio successivo sullo stesso database. Un container temporaneo usa l’immagine candidata e una directory Flyway aggiuntiva generata sul runner. La migrazione di prova (V21.1 con questo candidato) crea una tabella dedicata. La CI controlla la presenza della tabella, l’unica nuova riga SQL riuscita e l’integrità della cronologia precedente.
7. Fermato il container temporaneo, riparte lo stesso container candidato del passo 3 con le sole migrazioni del rilascio. Il job richiede HTTP 200, readiness `UP`, `FUTURE_MIGRATIONS_PRESENT`, conteggio 1 e cronologia identica a quella del passo 6.

Le scritture passano dai worker di attivazione, acquisizione e consegna dell'immagine. Le interrogazioni di controllo usano una sessione PostgreSQL in sola lettura. Il Gateway di test riprende il trasporto Bearer di `pairwise/r2b_serving.py`, la fixture CSV di `ManagedRepeatabilityRuntimeTest` e il formato di ricevuta firmata di `GatewayReadReceiptVerifierTest`. I peer di Onboarding, Semantic, object storage, Lake e UDP sono simulati sul runner. Token, chiave e grant di lettura sono locali alla prova.

**Sequenza di rilascio.** Il predecessore oggi in esercizio richiede la cronologia V21 esatta anche con `*:future`. Occorre quindi distribuire prima il rilascio del task-27b a schema invariato. Una volta in esercizio, potrà essere il predecessore di un candidato che estende lo schema. Per ogni coppia reale di revisioni, il job dovrà ancora verificare le letture, le scritture e la ripresa dell’handoff sui dati del candidato.
