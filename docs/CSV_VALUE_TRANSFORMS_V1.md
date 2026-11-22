# Trasformazioni dei valori CSV

Onboarding congela i lettori in `semanticMapping.propertyMappings` e i costruttori e i tipi strutturati in `semanticMapping.compositions`. Prontezza e handoff applicano lo stesso mapping agli UPSERT. La tabella riporta i cinque lettori storici. Il contratto comune elenca tutti i lettori, gli IRI di destinazione e le forme delle composizioni. [Contratto D-084](../contracts/ouf-value-types-v1.json)

| Regola | Datatype dichiarato | Esempio di cella CSV | Valore nel payload JSON |
|---|---|---|---|
| `IDENTITY` | `xsd:string` | `001.20` | `"001.20"` |
| `DECIMAL_DOT_V1` | `xsd:decimal` | `45.6500` | `"45.65"` |
| `DECIMAL_COMMA_V1` | `xsd:decimal` | `-12,3400` | `"-12.34"` |
| `DATE_ISO_V1` | `xsd:date` | `2024-02-29` | `"2024-02-29"` |
| `DATE_DMY_V1` | `xsd:date` | `29/02/2024` | `"2024-02-29"` |

**Decimali.** Dopo `strip()` sono ammessi un segno iniziale facoltativo (`+` o `-`), una o più cifre ASCII, anche con zeri iniziali, e una parte frazionaria facoltativa. Il separatore dichiarato richiede almeno una cifra dopo il punto o la virgola. Spazi interni, separatori di gruppo ed esponenti sono errori. Il limite è di 256 caratteri dopo `strip()`. Dopo la rimozione degli zeri frazionari finali, sono ammesse fino a 38 cifre significative, 38 cifre intere e 18 posizioni decimali. Lo zero con segno diventa `"0"`.

Il valore attraversa l’outbox come stringa decimale in base dieci. `BigDecimal.stripTrailingZeros().toPlainString()` conserva tutte le cifre anche quando un dispatcher, incluso il predecessore, deserializza il payload in una `Map` senza tipi numerici espliciti. Per le proprietà semplici UDP conserva il valore JSON e registra il datatype dichiarato nel profilo. Il comparatore di identità `DECIMAL_V1` accetta la stessa rappresentazione.

**Date.** Dopo `strip()`, entrambi i profili richiedono una data di calendario valida con giorno e mese di due cifre e anno da 0001 a 9999. La conversione produce una data ISO locale. I metadati temporali del record seguono le rispettive policy.

**Celle vuote ed errori.** Secondo `ouf.value-types.v1` (D-084), per decimali e date una cella `null` o una stringa vuota dopo `strip()` indica assenza. La pipeline omette la chiave dal record mappato e la prova di compatibilità accetta la riga. L’evidenza RAW conserva la cella originale. Un valore tipizzato non nullo di tipo diverso da stringa è un errore di dati. `IDENTITY` conserva stringa vuota, spazi e valori storici degli altri adapter, incluso `null`. Un campo sorgente mancante produce `ING_MAPPING_SOURCE_FIELD_MISSING`. Una regola sconosciuta produce `ING_TRANSFORM_UNSUPPORTED`.

Una conversione fallita blocca la prova di prontezza. La risposta HTTP 422 contiene `ING_TRANSFORM_VALUE_INVALID` e `rowOrdinal`, l’ordinale del record fornito dall’adapter (per il CSV, da 1 dopo l’intestazione). In esecuzione l’evidenza RAW conserva la cella originale. Il motivo di quarantena contiene il codice e il riferimento all’evidenza include l’ordinale. Il percorso predefinito mantiene il checkpoint e blocca la pubblicazione della riga.

Le composizioni possono usare campi inclusi nella proiezione anche quando il campo non ha una proprietà propria. I componenti seguono la definizione del `DATATYPE` fissata da `semanticId@publicationSetId`. Il gateway confronta la definizione e il digest con la risposta del Registry. La prova di prontezza richiede revisioni `ACTIVE`, incluse quelle annidate. La rilettura di una pubblicazione accetta anche definizioni `DEPRECATED` o `RETIRED`. L’envelope NORMALIZED punta al record RAW tramite `sourceDataRef`. Il lineage conserva il riferimento RAW e il suo SHA-256, così il valore sorgente resta recuperabile anche quando la rappresentazione canonica cambia. Le coordinate decimali restano numeri JSON esatti anche quando il dispatcher rilegge l’outbox.

`VALUE_MAP_V1` richiede le corrispondenze codice–IRI della pubblicazione fissata. Il bundle Onboarding attualmente include il pin e il vocabolario. Il lookup è verificato con tabelle in memoria, mentre l'integrazione richiede ancora un formato di pubblicazione per la tabella. Un pin privo della tabella produce `ING_VALUE_MAP_CONTENT_REQUIRED` prima della lettura dei dati.

**Verifica del contratto.** `OnboardingValueTransformContractTest` verifica lo SHA-256 e il catalogo dei lettori. Quando il ramo `main` di Onboarding contiene il contratto comune, la CI confronta i byte delle due copie. Durante il rollout, per un checkout Onboarding precedente a D-084 rimane attivo il confronto con le cinque trasformazioni di Onboarding.
