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

Con l'esecuzione abilitata, il dispatcher esamina una richiesta automatica di schema per tick (`ouf.ingestion.execution.review-delay-ms`, default 5000). Il client SERVICE usa il Gateway configurato e `/api/onboarding/v1/sources/{sourceId}/schema-surveillance`. Convalida la ricevuta 201 contro sorgente, riferimento, impronte, classificazione e dettaglio della richiesta. Lo stato della consegna e la risoluzione dell'incidente evolvono separatamente.

L'Onboarding di base crea un UUID per POST, anche a parità di contenuto. Per questo la prenotazione durevole precede l'unico POST. Un lease scaduto o una risposta ambigua attivano soltanto il GET di riconciliazione con la stessa chiave d'incidente. Una corrispondenza univoca consente di salvare la ricevuta. Se il processo cade tra prenotazione e invio e il GET non trova il report, la consegna resta esplicitamente da riconciliare. Per garantire anche il reinvio automatico in quel caso occorre deduplicazione atomica lato Onboarding. Una ricevuta confermata non concede approvazione HUMAN.

Le letture di riconciliazione transitorie hanno attese base 5/15/60/300/900 secondi, jitter fino al 20%, e rispettano Retry-After. Il budget persistente comprende al massimo 128 tentativi e 24 ore dal primo claim. L'ammissione riserva 15 secondi per la chiamata limitata a 12 secondi. Gli esiti terminali restano nell'outbox. L'owner Onboarding mantiene l'autorità sulla risoluzione delle sue issue.

La qualifica tra servizi richiede la rotta southbound della collezione, le rotte GET/POST della sorveglianza e il grant del workload. La gestione completa di watermark temporali, scadenza della retention, deduplica per revisione e cancellazioni da snapshot appartiene alla continuità delta. Il checkpoint congelato richiede che la sorgente conservi la posizione rigiocabile durante la pausa.
