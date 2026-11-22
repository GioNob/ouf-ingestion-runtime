# Azioni di recupero nelle letture operative

`POST /api/internal/v1/ingestion/operations/status` e `history` espongono le azioni dell’owner con la capability `ingestion.operations.read`. I campi legacy mantengono la forma e i valori precedenti. La proiezione è calcolata sullo schema V22, dopo la selezione della pagina per tenant. Le righe e i riferimenti THS passano dallo stesso filtro di visibilità esistente.

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

Un run in pausa espone `runId` ed `expectedVersion`. Impostare `OUF_ING_RECOVERY_THS_ORIGIN` all’origine HTTPS pubblica della THS del task-50b nella configurazione del workload. Ingestion aggiunge il percorso `/trusted-human/ingestion-run-recovery/` e la query `runId` e `expectedVersion`. Il link è un riferimento alla scheda autenticata e non attribuisce permessi. La scheda del task-50b gestisce le decisioni sul run. La revisione di un record in quarantena usa il percorso HUMAN della quarantena. Una schedulazione priva di run non emette un riferimento di decisione sul run.

`runState=COMPLETED` richiede un run riuscito, almeno una consegna, tutte le righe outbox con ACK e relativa ricevuta persistita e il `typeCode` del baseline immutabile. Il campo `udpHandoffConfirmed` è disponibile anche per `COMPLETED_WITH_WARNINGS`, che conserva il proprio stato. Un run vuoto può raggiungere `SUCCEEDED` senza passaggio a UDP. Il client usa `runState=COMPLETED` e `typeCode` per avanzare a `urban.object.search`. L’ACK conferma la consegna a UDP.
