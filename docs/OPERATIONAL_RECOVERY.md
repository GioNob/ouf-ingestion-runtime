# Azioni di recupero nelle letture operative

`POST /api/internal/v1/ingestion/operations/status` e `history` espongono le azioni dell’owner con la capability `ingestion.operations.read`. I campi legacy mantengono la forma e i valori precedenti. La proiezione usa lo schema V23 dopo la selezione della pagina per tenant e applica il filtro di visibilità esistente.

| `recoveryAction` | Evidenza operativa |
|---|---|
| `NONE` | Nessuna azione di recupero osservata. Può esserci lavoro ordinario o una transizione del worker ancora da registrare. |
| `AUTOMATIC_RETRY` | Il scheduler, il worker o il dispatcher ha un tentativo recuperabile nei limiti persistiti. |
| `AUTOMATIC_RECONCILIATION` | Il worker ha una scrittura Lake da riconciliare o il dispatcher può ripresentare la consegna UDP già tentata con esito incerto. |
| `HUMAN_RESUME_OR_ABORT` | I controlli locali consentono di sottoporre una decisione di ripresa o interruzione. La mutazione verifica autorità, versione e preflight. |
| `HUMAN_ABORT_REQUIRED` | I controlli della ripresa richiedono una disposizione degli effetti oppure manca il baseline. L’interruzione del run resta disponibile. |
| `HUMAN_QUARANTINE_REVIEW` | Almeno un record bloccante richiede una decisione di quarantena prima della ripresa. |
| `HUMAN_SCHEDULE_REVIEW` | La schedulazione richiede verifica operativa. Una riga di schedulazione può non avere un run controllabile. |
| `WAITING_FOR_PUBLICATION_VERIFICATION` | L’identità della pubblicazione coincide e la verifica di discovery deve essere rinnovata. |
| `NEW_PUBLICATION_REQUIRED` | Il run non può riprendere con la pubblicazione osservata, oppure la schedulazione bloccata non ha un run di ammissione da riprendere. Un run in pausa può ancora essere interrotto. |
| `TERMINAL` | Il run è concluso oppure la schedulazione è disabilitata. |

`retryAt` indica la prima eleggibilità in UTC per l’azione automatica esposta. Con più partizioni o fasi vale l’orario del primo candidato. Un lease, la verifica ACTIVE, la pressione di ammissione o un cambiamento concorrente possono rinviare il tentativo. Per esempio, dopo l’esaurimento del budget il worker può dover persistere la pausa alla prossima valutazione. I valori di `failure_code` sono testo aperto. L’azione usa stato, lease, ledger, quarantena e prove degli effetti. I codici speciali utilizzati dalle guardie conservano il significato del lifecycle.

`scheduleRecoveryAction` e, quando presente, `scheduleRetryAt` descrivono la schedulazione della pubblicazione corrente. Una riga senza run usa questi valori anche come `recoveryAction` e `retryAt`. Per esempio, il fallimento terminale di un run periodico può coesistere con un nuovo tentativo della schedulazione.

Il link alla scheda THS di recupero lo emette Onboarding (`POST /api/internal/v1/onboarding/ths-links/ingestion-run-recovery`). Ingestion espone `runId`, `expectedVersion`, `recoveryAction` e `decisionDeadline`. La revisione di un record in quarantena usa il percorso HUMAN della quarantena. Una schedulazione priva di run non emette un riferimento di decisione sul run.

`decisionDeadline` è presente per le quattro azioni `HUMAN_*` ed è in ISO-8601 UTC. Indica il termine minimo garantito dall’owner per la decisione osservata. Alla scadenza il lettore rivaluta lo stato. Il run o la schedulazione possono rimanere in attesa oltre il termine, senza abort o chiusure automatiche. Autorità, versione e preflight vengono verificati quando si sottopone la decisione.

Per un run vale il minimo tra l’ingresso nella pausa più `ouf.ingestion.recovery.human-decision-window` (default `PT24H`) e le scadenze applicabili. Con `HUMAN_RESUME_OR_ABORT` concorrono la sessione `ACTIVE` con tentativi residui, il budget di acquisizione delle partizioni in pausa con tentativi residui (`retry_started_at` più 24 ore) e il budget autorizzato della consegna in testa a ciascuna partizione, se ancora in corso e con tentativi residui. La freschezza della verifica di discovery non concorre: è una precondizione della ripresa rinnovata automaticamente dal ciclo di attivazione. Quando è scaduta l’azione diventa `WAITING_FOR_PUBLICATION_VERIFICATION`, senza `decisionDeadline`, e torna `HUMAN_RESUME_OR_ABORT` con lo stesso termine appena la verifica è rinnovata. Il termine può essere già trascorso e non scorre con le letture.

`scheduleDecisionDeadline` compare solo con `scheduleRecoveryAction=HUMAN_SCHEDULE_REVIEW`. Usa l’ingresso nella pausa della schedulazione oppure, per una schedulazione attiva bloccata da incidenti, la data più antica tra le pause dei run bloccanti, con `created_at` come evidenza stabile se il run non è ancora passato in pausa. Si somma la stessa finestra configurata. Una riga senza run ripete questo termine come `decisionDeadline`.

V23 registra le nuove pause con trigger, anche se scritte dal predecessore supportato `8be19a36`. Gli eventi operativi V6 possono ripetere `RUN_PAUSED` a ogni scrittura di stato o fase, e la schedulazione non ha un evento equivalente. Per le pause già presenti la migrazione conserva l’ultimo `updated_at` del run e il minimo tra le date disponibili per la schedulazione (`activation_first_failure_at`, `discovery_verified_at`, `last_dispatched_at`, `next_run_at`). Questi valori sono approssimazioni basate sui dati storici disponibili e vengono fissati durante la migrazione. Il job `predecessor-return` verifica la scrittura di una nuova pausa sullo schema V23 e la lettura della scadenza con il candidato.

`runState=COMPLETED` richiede un run riuscito, almeno una consegna, tutte le righe outbox con ACK e relativa ricevuta persistita e il `typeCode` del baseline immutabile. Il campo `udpHandoffConfirmed` è disponibile anche per `COMPLETED_WITH_WARNINGS`, che conserva il proprio stato. Un run vuoto può raggiungere `SUCCEEDED` senza passaggio a UDP. Il client usa `runState=COMPLETED` e `typeCode` per avanzare a `urban.object.search`. L’ACK conferma la consegna a UDP.
