# Sorveglianza dello schema PULL (D-076, D-077)

## Lettura e confronto

Il worker legge la pagina REST dal binding già approvato. Prima di estrarre identità, revisioni o proprietà legge la dichiarazione della stessa collezione con lo stesso binding. Su `POST /internal/sources/v1/fetch` i parametri per questa seconda lettura sono `{"resource":"schema","collection":"…"}`. L'adattatore southbound del Gateway deve tradurli in `GET /collections/{c}/schema`. Per le istanze usa `limit`, `collection` quando fissata e il cursore opaco. La pagina già letta alimenta il consumo. Anche le pagine successive e quelle vuote attraversano il controllo. Per un WFS pubblicato il binding deve fornire la dichiarazione equivalente prima di GetFeature.

Il riferimento prioritario è il profilo immutabile del bundle, in particolare `extractionProfile.sourceUnitProfile.readParameters.api.{schemaVersion,fieldsSha256}`. La chiave viene da `keys.declared[0]` e la geometria da `geometry`. Il digest copre il JSON UTF-8 compatto di tutti i campi dichiarati, ordinati per nome, con proprietà ordinate per chiave. Si verificano anche la collezione e l'accordo tra versione della busta e versione della dichiarazione.

Per i bundle storici si conservano `runtime.execution.sourceSchemaRef`, `sourceSchemaVersion`, i nomi della proiezione e la chiave della pubblicazione. Se l'esecuzione include la dichiarazione approvata in `sourceSchema`, il worker usa `fieldsSha256` oppure calcola il digest di `fields`, con `key` e `geometry` della dichiarazione. Un riferimento opaco e una lista di nomi da soli lasciano il consumo in osservazione con `APPROVED_FINGERPRINT_MISSING`. La prima risposta del verticale non può approvare se stessa. Un produttore R2b con sole righe piatte necessita della busta e di una pubblicazione con evidenza tipizzata per il polling governato.

## Checkpoint, avviso e ripresa

La chiave è la coppia tenant e sorgente. Il catalogo assegna uno schedule per sorgente e il runtime crea la partizione `default`. La migrazione V19 conserva il checkpoint e applica un indice univoco per incidente aperto. Una singola transazione verifica il lease del worker e la pubblicazione corrente, mette in pausa il run e registra issue, osservazione e richiesta di consegna. Le richieste HTTP si svolgono fuori dalla transazione. Solo un nuovo slot del poll può recuperare il blocco per schema.

Un nuovo poll riparte dal checkpoint congelato. In CURSOR, in assenza di blocco, riparte dalla posizione associata all'ultimo handoff del prefisso confermato. Il cursore restituito sull'ultima pagina resta utilizzabile anche con `hasMore=false`. Gli ACK precedenti ancora pendenti devono arrivare prima della ripresa. La chiusura dell'incidente è atomica con il primo checkpoint del run ammesso o con il completamento di una lettura vuota. `resume_run` collega la risoluzione al bundle effettivamente eseguito.

La vista owner `ouf_ingestion.pull_schema_notice` contiene solo blocchi aperti. L'API esistente `GET /api/trusted-human/v1/ingestion/issues/{id}/explain` espone tenant, sourceId, schemi, differenze, contatore, tempi, azione `REVISE_ONBOARDING`, stato consegna e prova di ripresa. La lettura lascia aperto l'avviso. La scelta dei revisori e la presentazione al login competono a Gateway, Onboarding, MCP e THS.

## Segnalazione e confini di integrazione

Con l'esecuzione abilitata, il dispatcher esamina una richiesta automatica di schema per tick (`ouf.ingestion.execution.review-delay-ms`, default 5000). Il client SERVICE usa il Gateway configurato e `/api/onboarding/v1/sources/{sourceId}/schema-surveillance`. Convalida la ricevuta 201 contro sorgente, riferimento, impronte, classificazione e dettaglio della richiesta. Lo stato della consegna e la risoluzione dell'incidente evolvono separatamente.

L'Onboarding di base crea un UUID per POST, anche a parità di contenuto. Per questo la prenotazione durevole precede l'unico POST. Un lease scaduto o una risposta ambigua attivano soltanto il GET di riconciliazione con la stessa chiave d'incidente. Una corrispondenza univoca consente di salvare la ricevuta. Se il processo cade tra prenotazione e invio e il GET non trova il report, la consegna resta esplicitamente da riconciliare. Per garantire anche il reinvio automatico in quel caso occorre deduplicazione atomica lato Onboarding. Una ricevuta confermata non concede approvazione HUMAN.

Le letture di riconciliazione transitorie hanno attese base 5/15/60/300/900 secondi, jitter fino al 20%, e rispettano Retry-After. Il budget persistente comprende al massimo 128 tentativi e 24 ore dal primo claim. L'ammissione riserva 15 secondi per la chiamata limitata a 12 secondi. Gli esiti terminali restano nell'outbox. L'owner Onboarding mantiene l'autorità sulla risoluzione delle sue issue.

La qualifica tra servizi richiede la rotta southbound della collezione, le rotte GET/POST della sorveglianza e il grant del workload. La gestione completa di watermark temporali, scadenza della retention, deduplica per revisione e cancellazioni da snapshot appartiene alla continuità delta. Il checkpoint congelato richiede che la sorgente conservi la posizione rigiocabile durante la pausa.
