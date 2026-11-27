# Verifica dei riferimenti semantici

La configurazione congelata e la pubblicazione devono entrare nella lettura da 3 MiB. Il numero dei riferimenti esatti deriva dal bundle pubblicato da Onboarding. Ingestion controlla tutti i pin a ogni prepare, preflight, lettura storica e riverifica della pubblicazione dopo una sospensione.

Il client invia tuple ordinate a `POST /api/semantic/v1/references:resolve-batch` secondo `ouf.semantic.reference-batch.v1`. Conta i byte UTF-8 del corpo completo. Un 413 con codice `SEM_REFERENCE_BATCH_TOO_LARGE` dimezza il numero di elementi del blocco e prosegue dal primo elemento ancora da verificare. Un singolo pin troppo grande produce `ING_SEMANTIC_REFERENCE_TOO_LARGE`. Con una risposta incoerente per ordine, cardinalità o versione, il client riparte dal primo pin fino a tre verifiche complete, poi espone `ING_REFERENCE_BATCH_INCONSISTENT`.

Lo stato ammesso dipende dalla modalità di risoluzione. Il digest D-084 della definizione incorporata di DATATYPE e VALUE_MAP deve coincidere con il digest pubblicato. Per un CONCEPT il lotto restituisce la stessa definizione del GET, compreso `vocabularyRef`: il client verifica l'appartenenza al vocabolario confrontandola con la definizione incorporata, senza GET per elemento. Una riga CONCEPT senza definizione viola il contratto e fa rileggere l'intera verifica. Il GET per elemento, dosato, resta solo come ricaduta quando l'endpoint a lotti manca (404 o 405).

Un solo pin non verificato blocca l’ammissione dell’intera fonte o la ripresa del run. La readiness e le proiezioni `status` e `history` espongono in `semanticReference` il codice dell’elemento, l’indice globale a base zero, `semanticId` e `revisionId`. `ING_SEMANTIC_REFERENCE_UNRESOLVABLE` identifica un esito `NOT_RESOLVABLE` o un GET puntuale 404. `ING_SEMANTIC_REFERENCE_MISMATCH` identifica una discrepanza di identità, versione, stato o definizione. Un 429, 503 o timeout interrompe la verifica del lotto. La ripresa rilegge tutti i blocchi con un nuovo controllo di `registryVersion`. Se il blocco avviene in preflight, lake e outbox restano vuoti per il run. Se avviene alla riverifica, restano validi gli effetti già committati prima della pausa.

Per D-080, il run, la readiness e l’evento mantengono il codice esterno preesistente, incluso `ING_SEMANTIC_BINDING_MISMATCH` o `ING_VALUE_DEFINITION_INVALID` secondo la causa. Il nuovo codice è nel dettaglio dell’elemento. Nelle copie locali verificate, Onboarding classifica la risposta di readiness tramite lo stato HTTP in `ManagedActivationCoordinator.post`. In MCP non sono emersi confronti con questi due codici. Gli eventi già salvati restano leggibili con i soli campi disponibili.

## Aggiornamento graduale

Se il POST riceve 404 o 405, il processo usa il GET esatto per tutti gli elementi della verifica. Dopo il TTL riprova il POST. Lo stato memorizzato riguarda solo la disponibilità della rotta. Tra due verifiche tutti i risultati vengono riletti. Un vecchio Semantic, con molti pin e un limite basso, può superare la durata della lease o della readiness. In esercizio aggiornare prima Semantic e Gateway.

## Dosaggio e ripresa

Ogni processo applica un ritmo distinto a lake, handoff e riferimenti (GET e POST condividono la stessa classe). Su una risposta riuscita, Remaining e Reset possono essere frazionari. Il dosatore divide il tempo residuo per la quota disponibile e attende il reset se rimane meno di una richiesta. Repliche e altri consumatori del Gateway possono utilizzare contemporaneamente la stessa quota. L’installazione deve ripartire il budget fra processi. I parametri sono nel [registro D-065](CONFIGURATION_REGISTER.md).

Per un transitorio prevale il primo header valido nell’ordine `Retry-After` (secondi interi o data RFC 1123), `X-RateLimit-Reset`, `RateLimit-Reset`. Entrambi i reset accettano secondi decimali e indicano un’epoca Unix da 1 000 000 000 in su. Senza header validi vale l’intervallo configurato della rotta. Il client campiona il jitter fino al 20% una sola volta e applica il tetto configurato al risultato. La partizione arrotonda l’attesa al secondo superiore.

Il worker confronta il checkpoint JSONB persistito sotto fence con quello letto al claim. Un avanzamento azzera la serie di fallimenti consecutivi. Senza progresso resta il backoff esistente. I tentativi cumulativi e la scadenza del run restano durevoli. Le scritture lake ambigue seguono la riconciliazione per chiave e contenuto del task-48.
