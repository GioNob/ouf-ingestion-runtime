# Ritorno al predecessore di Ingestion (D-080)

Il predecessore supportato per questo rilascio è `6f0504240b2653e5aa93729ae70cedc52228515d`, in esercizio dal 21 novembre 2026 con schema V21. Il candidato di task-27 conserva V21. Il riferimento di CI è lo SHA completo nel job `predecessor-return` di [module-ci.yml](../.github/workflows/module-ci.yml).

## Regola per le migrazioni

Ogni rilascio deve permettere di riavviare il predecessore sul database già migrato e popolato dal candidato. Durante la fase *expand*, il candidato conserva colonne, vincoli, viste e significato dei valori che il predecessore legge o scrive. La fase *contract* e l'abilitazione dei nuovi stati avvengono in un rilascio successivo, quando il codice in esercizio può gestirli.

Ogni task con migrazioni dichiara il predecessore supportato, le sue dipendenze SQL, i nuovi stati persistiti e il rilascio che ne autorizza la scrittura. Il piano di rilascio deve rifiutare i salti di versione privi di prova. Una trasformazione irreversibile richiede un mandato distinto e approvazione HUMAN esplicita. Si dichiara `rollbackCompatible: true` solo dopo la prova riuscita per quella coppia di revisioni.

## Prova CI

Il job gira su push e pull request. Nel lancio manuale il parametro `predecessor` accetta uno SHA Git. Il candidato è sempre il `HEAD` del checkout del job. Quando cambia la versione in esercizio, si aggiornano insieme il pin nel workflow e questa pagina.

1. La CI costruisce ogni immagine dall'archivio Git del suo commit con il Dockerfile di quella revisione. Verifica `org.opencontainers.image.revision` e registra SHA e image ID nel log.
2. Il predecessore parte su un PostgreSQL 17 dedicato e vuoto. Flyway installa le migrazioni. Il runtime elabora una pubblicazione CSV e porta run e partizione a `SUCCEEDED`, con handoff `ACKED` e watermark associato.
3. Il candidato parte sullo stesso database, applica le proprie migrazioni e legge il run precedente via API. Elabora una seconda pubblicazione. La fixture risponde HTTP 503 con `Retry-After` alla consegna. La CI attende `FAILED_RETRYABLE` persistito e controlla che il watermark punti ancora al primo handoff.
4. Dopo lo stop del candidato riparte lo stesso container predecessore, con Flyway attivo e `*:future`. La CI richiede readiness `UP` e la stessa cronologia Flyway lasciata dal candidato. Il predecessore deve ritentare la consegna con lo stesso payload, handoff ID e chiave persistita, ottenere l'ACK e avanzare il watermark al secondo handoff.
5. Le API autenticate `status` e `history` devono restituire gli ID delle pubblicazioni e dei run attesi. Una terza pubblicazione deve completarsi sul predecessore. Il totale finale è di tre run, tre partizioni e tre righe di outbox.

Le scritture passano dai worker di attivazione, acquisizione e consegna dell'immagine. Le interrogazioni di controllo usano una sessione PostgreSQL in sola lettura. Il Gateway di test riprende il trasporto Bearer di `pairwise/r2b_serving.py`, la fixture CSV di `ManagedRepeatabilityRuntimeTest` e il formato di ricevuta firmata di `GatewayReadReceiptVerifierTest`. I peer di Onboarding, Semantic, object storage, Lake e UDP sono simulati sul runner. Token, chiave e grant di lettura sono locali alla prova.

**Limite di V21.** Il criterio di readiness del predecessore richiede la cronologia Flyway esatta incorporata in `IngestionReadinessHealthIndicator`. Una migrazione successiva a V21 aggiunge una riga che quel codice rifiuta anche con `*:future`. Il gate deve fallire in tale situazione. Il primo rilascio che estende lo schema richiede prima un predecessore in esercizio capace di accettare la nuova cronologia.
