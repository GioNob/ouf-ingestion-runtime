# Trasformazioni dei valori CSV

Onboarding congela una regola per ogni `propertyMappings[].transform`. Ingestion la applica agli UPSERT nello stesso punto della pipeline durante la prontezza e durante la preparazione dell’handoff. Il contratto elenca gli identificatori esatti e gli IRI dei datatype di destinazione. [Contratto v1](../contracts/onboarding-value-transforms-v1.json)

| Regola | Datatype dichiarato | Esempio di cella CSV | Valore nel payload JSON |
|---|---|---|---|
| `IDENTITY` | `xsd:string` | `001.20` | `"001.20"` |
| `DECIMAL_DOT_V1` | `xsd:decimal` | `45.6500` | `"45.65"` |
| `DECIMAL_COMMA_V1` | `xsd:decimal` | `-12,3400` | `"-12.34"` |
| `DATE_ISO_V1` | `xsd:date` | `2024-02-29` | `"2024-02-29"` |
| `DATE_DMY_V1` | `xsd:date` | `29/02/2024` | `"2024-02-29"` |

**Decimali.** Il profilo di `bcc1354` ammette cifre ASCII, un segno meno facoltativo e una parte frazionaria facoltativa. Servono almeno una cifra intera e, se presente il separatore dichiarato, almeno una cifra frazionaria. Spazi, separatori di gruppo, esponenti, segno più e zeri iniziali multipli sono errori. L’ingresso ha un limite di 256 caratteri. Dopo la rimozione degli zeri frazionari finali, sono ammesse fino a 38 cifre significative, 38 cifre intere e 18 posizioni decimali. Lo zero con segno diventa `"0"`.

Il valore attraversa l’outbox come stringa decimale in base dieci. `BigDecimal.stripTrailingZeros().toPlainString()` conserva tutte le cifre anche quando un dispatcher, incluso il predecessore, deserializza il payload in una `Map` senza tipi numerici espliciti. Per le proprietà semplici UDP conserva il valore JSON e registra il datatype dichiarato nel profilo. Il comparatore di identità `DECIMAL_V1` accetta la stessa rappresentazione.

**Date.** Entrambi i profili richiedono una data di calendario valida con giorno e mese di due cifre e anno da 0001 a 9999. La conversione produce una data ISO locale. I metadati temporali del record seguono le rispettive policy.

**Celle vuote ed errori.** Come in `bcc1354`, `IDENTITY` conserva stringa vuota, spazi e valori storici degli altri adapter, incluso `null`. Per decimali e date la cella vuota, il valore nullo e un ingresso diverso da una stringa sono errori di dati. Un campo sorgente assente produce `ING_MAPPING_SOURCE_FIELD_MISSING`. Una regola sconosciuta produce `ING_TRANSFORM_UNSUPPORTED`.

Una conversione fallita blocca la prova di prontezza. La risposta HTTP 422 contiene `ING_TRANSFORM_VALUE_INVALID` e `rowOrdinal`, l’ordinale del record fornito dall’adapter (per il CSV, da 1 dopo l’intestazione). In esecuzione l’evidenza RAW conserva la cella originale. Il motivo di quarantena contiene il codice e il riferimento all’evidenza include l’ordinale. Il percorso predefinito mantiene il checkpoint e blocca la pubblicazione della riga.

**Verifica del contratto.** `OnboardingValueTransformContractTest` esercita ogni voce del contratto. Con `OUF_ONBOARDING_CHECKOUT` valorizzata confronta tutte le coppie identificatore/datatype con `ManagedFileExecutionProposal.DATATYPES` nel checkout indicato. La CI di modulo esegue questo confronto con il ramo `main` di Onboarding. Per eseguire la prova localmente, impostare la variabile sulla directory del repository proprietario ed eseguire `mvn -Dtest=OnboardingValueTransformContractTest test`.
