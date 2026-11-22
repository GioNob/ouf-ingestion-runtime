# Registro della configurazione di Ingestion

| Risorsa | Tipo e contenuto | Segreti | Cattura e ripristino |
|---|---|---|---|
| `ouf.ingestion.activation.token-file` | File montato con il bearer del workload usato per Gateway. Il percorso proviene dal profilo di installazione. | S? | Acquisire il riferimento al mount, il proprietario, i permessi e la configurazione del servizio emittente. Escludere il bearer da backup, log e snapshot. Al ripristino, ristabilire il mount e far emettere una credenziale fresca. |
| `ouf.ingestion.recovery.human-decision-window` | Parametro dell’installazione D-065. Durata ISO-8601 positiva, default `PT24H` (`OUF_ING_RECOVERY_HUMAN_DECISION_WINDOW`). Limita la finestra garantita per la decisione umana letta in `status` e `history`. | Nessuno. | Registrare il valore effettivo nel profilo dell’installazione e ripristinarlo con lo stesso profilo. |
| `ouf_ingestion.lake_write_evidence` | Stato operativo PostgreSQL. La chiave identifica run, partizione, sequenza ed effetto. La riga associa hash, eventuale ricevuta, causa e indicazioni di intervento. | Nessuna credenziale. Gli identificativi restano nel perimetro operativo del tenant. | Acquisire e ripristinare insieme a run, checkpoint, snapshot, sessioni di recupero e outbox nello stesso backup PostgreSQL. |

Il servizio che rinnova il bearer deve scrivere il file completo in un temporaneo nella stessa directory, applicare i permessi previsti, chiuderlo e sostituire il nome di destinazione con una rinomina atomica. Il mount deve rendere visibile la sostituzione. Una stringa opaca troncata pu? avere la stessa sintassi di una credenziale completa. La rinomina ? quindi un requisito di installazione anche con la rilettura del client.

Il profilo di lettura locale v1 ammette tre letture con attese di 25 e 50 ms dopo un errore. Esaurite le letture, il worker rilascia la lease e usa il budget del run. Una risposta di diniego del Gateway mantiene la propria classificazione.

D-085 ritira la chiave `ouf.ingestion.recovery.ths-origin` (`OUF_ING_RECOVERY_THS_ORIGIN`) dal registro D-065. Rimuoverla dai profili Ingestion salvati. Onboarding gestisce l’origine della THS tramite la propria configurazione.
