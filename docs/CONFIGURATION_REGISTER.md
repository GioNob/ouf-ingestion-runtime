# Registro della configurazione di Ingestion

| Risorsa | Tipo e contenuto | Segreti | Cattura e ripristino |
|---|---|---|---|
| `ouf.ingestion.activation.token-file` | File montato con il bearer del workload usato per Gateway. Il percorso proviene dal profilo di installazione. | S? | Acquisire il riferimento al mount, il proprietario, i permessi e la configurazione del servizio emittente. Escludere il bearer da backup, log e snapshot. Al ripristino, ristabilire il mount e far emettere una credenziale fresca. |
| `ouf.ingestion.recovery.human-decision-window` | Parametro dell’installazione D-065. Durata ISO-8601 positiva, default `PT24H` (`OUF_ING_RECOVERY_HUMAN_DECISION_WINDOW`). Limita la finestra garantita per la decisione umana letta in `status` e `history`. | Nessuno. | Registrare il valore effettivo nel profilo dell’installazione e ripristinarlo con lo stesso profilo. |
| `ouf_ingestion.lake_write_evidence` | Stato operativo PostgreSQL. La chiave identifica run, partizione, sequenza ed effetto. La riga associa hash, eventuale ricevuta, causa e indicazioni di intervento. | Nessuna credenziale. Gli identificativi restano nel perimetro operativo del tenant. | Acquisire e ripristinare insieme a run, checkpoint, snapshot, sessioni di recupero e outbox nello stesso backup PostgreSQL. |

Il servizio che rinnova il bearer deve scrivere il file completo in un temporaneo nella stessa directory, applicare i permessi previsti, chiuderlo e sostituire il nome di destinazione con una rinomina atomica. Il mount deve rendere visibile la sostituzione. Una stringa opaca troncata pu? avere la stessa sintassi di una credenziale completa. La rinomina ? quindi un requisito di installazione anche con la rilettura del client.

Il profilo di lettura locale v1 ammette tre letture con attese di 25 e 50 ms dopo un errore. Esaurite le letture, il worker rilascia la lease e usa il budget del run. Una risposta di diniego del Gateway mantiene la propria classificazione.

D-085 ritira la chiave `ouf.ingestion.recovery.ths-origin` (`OUF_ING_RECOVERY_THS_ORIGIN`) dal registro D-065. Rimuoverla dai profili Ingestion salvati. Onboarding gestisce l’origine della THS tramite la propria configurazione.

## Parametri Gateway del worker

Proprietà intere positive con prefisso `ouf.ingestion.gateway.`. Registrare e ripristinare ciascun valore nel profilo di installazione. I valori seguenti sono default di sviluppo. Nessun parametro contiene credenziali.

| Proprietà dopo il prefisso | Default | Scopo |
|---|---:|---|
| `reference-batch-bytes` | 262144 | Massimo di byte UTF-8 per corpo POST completo. |
| `reference-batch-items` | 1000 | Numero massimo di tuple per blocco, da 1 a 1000. |
| `reference-fallback-seconds` | 300 | Durata in secondi della ricaduta GET dopo un POST 404 o 405. |
| `lake-per-minute` | 60 | Ritmo massimo per processo delle scritture lake. |
| `handoff-per-minute` | 60 | Ritmo massimo per processo delle consegne handoff. |
| `references-per-minute` | 60 | Ritmo massimo per processo delle richieste di risoluzione GET e POST. |
| `other-retry-per-minute` | 60 | Ritmo di ripresa per le rotte Gateway senza dosatore dedicato. Senza header validi, l’intervallo di base è 60 secondi diviso per il ritmo. |
| `server-wait-max-seconds` | 86400 | Tetto in secondi dell’attesa indicata dal Gateway, incluso il jitter, e della finestra usata dal dosatore. Il backoff senza progresso e i budget durevoli si applicano separatamente. |

## Limiti di `managed-xlsx-v1`

Worker e probe leggono gli stessi default da `managed-xlsx.properties`. La proprietà JVM con prefisso `ouf.ingestion.xlsx.` ha precedenza sulla variabile d’ambiente indicata. Registrare i valori effettivi nel profilo d’installazione e ripristinarli per entrambi i processi. I sei parametri sono pubblici e positivi.

| Proprietà dopo il prefisso | Variabile d’ambiente | Default | Massimo di protocollo |
|---|---|---:|---:|
| `max-file-bytes` | `OUF_ING_XLSX_MAX_FILE_BYTES` | 10485760 | 10485760 |
| `max-total-bytes` | `OUF_ING_XLSX_MAX_TOTAL_BYTES` | 104857600 | 268435456 |
| `max-entry-bytes` | `OUF_ING_XLSX_MAX_ENTRY_BYTES` | 67108864 | 268435456 |
| `min-inflate-ratio` | `OUF_ING_XLSX_MIN_INFLATE_RATIO` | 0.01 | — |
| `max-entries` | `OUF_ING_XLSX_MAX_ENTRIES` | 10000 | 10000 |
| `max-sheets` | `OUF_ING_XLSX_MAX_SHEETS` | 64 | 64 |

Il rapporto è il numero di byte compressi diviso per i byte decompressi della singola voce. Il controllo del totale conta tutti i byte decompressi durante la verifica dell’archivio. Il conteggio dei fogli include i fogli di lavoro nascosti ed esclude i fogli grafico.

Il bundle congela `maxRows`, `maxColumns`, `maxCellChars` e i parametri del foglio in `readParameters.xlsx`. Il solo fuso applicabile alle celle con data e ora è `readParameters.xlsx.timeZone`.
