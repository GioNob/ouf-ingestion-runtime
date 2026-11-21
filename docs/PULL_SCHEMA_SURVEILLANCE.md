# Sorveglianza dello schema PULL (D-076, D-077)

## Lettura e confronto

Il worker applica D-076 quando riconosce `ouf.vertical-source.v1` da almeno una delle seguenti evidenze.

- Il binding lo dichiara con `sourceContract: "ouf.vertical-source.v1"` nella configurazione di esecuzione approvata (`extractionProfile.runtime.execution` per un bundle pubblicato), oppure la pubblicazione riporta lo stesso `sourceContract` alla radice del bundle.
- Il profilo immutabile dell'unità dichiara `readParameters.api.schemaVersion`, il campo che il contratto del profilo riserva allo schema della collezione API.
- La pagina REST contiene il membro JSON `schemaVersion` alla radice. Anche un valore nullo o malformato attiva la verifica e impedisce di consumare la pagina come storica.

La versione `runtime.execution.sourceSchemaVersion` identifica anche schemi storici. Il solo nome della collezione e i campi dentro `items` possono appartenere a dati ordinari. Per questo nessuno di questi elementi seleziona il protocollo verticale.

**REST storica.** Il primo fetch usa `pageSize` e l'eventuale `pageToken` del checkpoint. Se la busta è priva di `schemaVersion` e manca una dichiarazione del contratto, il worker legge le righe piatte con i puntatori configurati, valida la pagina e osserva i nomi dei campi di ciascun record secondo la proiezione approvata. Una pagina vuota termina o prosegue secondo il token successivo. In tutti questi casi le sole richieste alla fonte sono quelle delle pagine dei dati. Il WFS storico effettua direttamente GetFeature e conserva la sorveglianza per record.

**Verticale.** Quando il contratto è già noto, il worker REST chiede le istanze con `limit`, la `collection` se fissata e l'eventuale cursore opaco. Se riconosce il contratto solo dalla risposta, riusa la pagina appena letta. Prima di estrarre i record, chiede la dichiarazione della collezione allo stesso binding con `{"resource":"schema","collection":"…"}` su `POST /internal/sources/v1/fetch`. Il Gateway traduce la richiesta in `GET /collections/{c}/schema`. La verifica si ripete sulle pagine successive e vuote. Un checkpoint verticale conserva il protocollo alla ripartenza. La perdita della versione dopo il riconoscimento mantiene il controllo attivo. Un incidente già aperto richiede ancora una prova di uguaglianza per riprendere. Un WFS che dichiara il contratto deve fornire la dichiarazione equivalente prima di GetFeature.

Il riferimento prioritario è il profilo immutabile del bundle, in particolare `extractionProfile.sourceUnitProfile.readParameters.api.{schemaVersion,fieldsSha256}`. La chiave viene da `keys.declared[0]` e la geometria da `geometry`. Il digest copre il JSON UTF-8 compatto di tutti i campi dichiarati, ordinati per nome, con proprietà ordinate per chiave. Si verificano anche la collezione e l'accordo tra versione della busta e versione della dichiarazione.

Per un verticale con un bundle privo del profilo dell'unità, il worker usa la dichiarazione approvata in `runtime.execution.sourceSchema`, con `fieldsSha256` oppure un digest calcolato da `fields`, la chiave e la geometria. Se la pubblicazione offre soltanto un riferimento e i nomi della proiezione, la verifica verticale rileva `APPROVED_FINGERPRINT_MISSING`. L'approvazione dell'impronta deve provenire dalla pubblicazione.

## Checkpoint, avviso e ripresa

La chiave è la coppia tenant e sorgente. Il catalogo assegna uno schedule per sorgente e il runtime crea la partizione `default`. La migrazione V19 conserva il checkpoint e applica un indice univoco per incidente aperto. Una singola transazione verifica il lease del worker e la pubblicazione corrente, mette in pausa il run e registra issue, osservazione e richiesta di consegna. Le richieste HTTP si svolgono fuori dalla transazione. Solo un nuovo slot del poll può recuperare il blocco per schema.

Un nuovo poll riparte dal checkpoint congelato. In CURSOR, in assenza di blocco, riparte dalla posizione associata all'ultimo handoff del prefisso confermato. Il cursore restituito sull'ultima pagina resta utilizzabile anche con `hasMore=false`. Gli ACK precedenti ancora pendenti devono arrivare prima della ripresa. La chiusura dell'incidente è atomica con il primo checkpoint del run ammesso o con il completamento di una lettura vuota. `resume_run` collega la risoluzione al bundle effettivamente eseguito.

La vista owner `ouf_ingestion.pull_schema_notice` contiene solo blocchi aperti. L'API esistente `GET /api/trusted-human/v1/ingestion/issues/{id}/explain` espone tenant, sourceId, schemi, differenze, contatore, tempi, azione `REVISE_ONBOARDING`, stato consegna e prova di ripresa. La lettura lascia aperto l'avviso. La scelta dei revisori e la presentazione al login competono a Gateway, Onboarding, MCP e THS.

## Segnalazione e confini di integrazione

Il dispatcher SERVICE consegna un evento per tick (default 5 s) via Gateway a `POST /api/onboarding/v1/sources/{sourceId}/schema-surveillance`. La V20 usa il contratto `ouf.schema-notice.v1` del task-05. La chiave `ingestion:<tenant>:<sourceId>:<issueId>` identifica l'incidente presso Onboarding. Ogni transizione ha una propria chiave HTTP di idempotenza che resta stabile nei tentativi successivi.

- **OPEN.** Nella transazione di pausa il produttore congela lo schema osservato, compresi i descrittori completi coperti dal digest, le differenze per campo e il riferimento al checkpoint conservato in `pull_schema_incident.frozen_checkpoint`. Il contatore parte da uno. Se la pubblicazione fissa soltanto il digest, l'ultimo insieme di descrittori verificati per tenant e sorgente può fornire la base del confronto quando il digest coincide.
- **UPDATE.** Il contatore cresce una volta per run di osservazione, anche durante l'attesa degli ACK del prefisso. La proprietà `ouf.ingestion.schema-notice.update-min-seconds` viene fissata all'apertura. Il default di zero secondi rende visibile ogni nuovo poll alla cadenza già stabilita dallo scheduler. Con un intervallo maggiore, il prossimo evento ammesso porta il totale e l'istante correnti. In presenza di arretrato restano al massimo due UPDATE da consegnare per incidente. Il primo mantiene il suo posto, il secondo raccoglie i poll successivi finché è disponibile e mai prenotato.
- **RESOLVED.** Il primo checkpoint durevole o il drain di una pagina vuota accodano la chiusura nella stessa transazione. Il motivo è `REVISION_ACTIVATED` quando il nuovo bundle ha cambiato l'approvazione che bloccava il consumo, altrimenti è `SCHEMA_RETURNED`. La chiusura include anche i poll accorpati. Il corpo di ogni POST riusa l'evidenza dell'apertura.

Il dispatcher consegna le transizioni in ordine per incidente. Dopo una prenotazione scaduta o una risposta persa, il GET SERVICE cerca una sola ricevuta con `incident_key` nella sorgente autenticata. Verifica sorgente, UUID owner (`issue_id`), impronta e progresso della transizione. Un UPDATE richiede contatore e istante almeno pari a quelli inviati, oppure una chiusura già registrata dall'owner. Se manca la prova, il worker verifica nuovamente il lease e può ripetere il POST v1 con lo stesso corpo e la stessa chiave. La deduplicazione atomica dell'apertura e gli aggiornamenti cumulativi di Onboarding rendono sicura anche la caduta prima del primo invio.

I tentativi hanno attese base di 5/15/60/300/900 s, jitter fino al 20% e rispettano Retry-After. Restano i limiti durevoli di 128 tentativi e 24 ore per transizione. L'esaurimento o un errore permanente arrestano anche le transizioni successive dello stesso incidente. Le operazioni di rete avvengono fuori dalle transazioni DB.

L'owner richiede almeno una differenza per campo fra i sei codici v1. Un cambio di sola versione, una dichiarazione malformata o un digest variato senza descrittori sufficienti per provare la differenza possono bloccare il consumo senza consentire un OPEN valido. In questi casi l'issue conserva l'evidenza e la consegna espone `FAILED_TERMINAL / ING_SCHEMA_NOTICE_EVIDENCE_INCOMPLETE`. Le aperture storiche già persistite in V19 conservano il loro protocollo e la loro ricevuta.

La fixture `src/test/resources/onboarding-schema-surveillance.json` riproduce il requestBody JSON del POST e i tre schemi referenziati in `openapi/onboarding-v1.yaml` di Onboarding, revisione `df66dc2`. Il test del worker valida i corpi passati al Gateway contro questa fixture. La prova tra servizi richiede le rotte Gateway e il grant del workload.
