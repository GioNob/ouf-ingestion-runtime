# Trasformazioni dei valori CSV

Onboarding congela i lettori in `semanticMapping.propertyMappings` e i costruttori e i tipi strutturati in `semanticMapping.compositions`. Prontezza e handoff applicano lo stesso mapping agli UPSERT. La tabella riporta i cinque lettori storici. Il contratto comune elenca tutti i lettori, gli IRI di destinazione e le forme delle composizioni. [Contratto D-084](../contracts/ouf-value-types-v1.json)

| Regola | Datatype dichiarato | Esempio di cella CSV | Valore nel payload JSON |
|---|---|---|---|
| `IDENTITY` | `xsd:string` | `001.20` | `"001.20"` |
| `DECIMAL_DOT_V1` | `xsd:decimal` | `45.6500` | `"45.65"` |
| `DECIMAL_COMMA_V1` | `xsd:decimal` | `-12,3400` | `"-12.34"` |
| `DATE_ISO_V1` | `xsd:date` | `2024-02-29` | `"2024-02-29"` |
| `DATE_DMY_V1` | `xsd:date` | `29/02/2024` | `"2024-02-29"` |

**Decimali.** Dopo il trim sono ammessi un segno iniziale facoltativo (`+` o `-`), una o più cifre ASCII, anche con zeri iniziali, e una parte frazionaria facoltativa. Il separatore dichiarato richiede almeno una cifra dopo il punto o la virgola. Spazi interni, separatori di gruppo ed esponenti sono errori. Il limite è di 256 caratteri dopo il trim. Dopo la rimozione degli zeri frazionari finali, sono ammesse fino a 38 cifre significative, 38 cifre intere e 18 posizioni decimali. Lo zero con segno diventa `"0"`.

Il valore attraversa l’outbox come stringa decimale in base dieci. `BigDecimal.stripTrailingZeros().toPlainString()` conserva tutte le cifre anche quando un dispatcher, incluso il predecessore, deserializza il payload in una `Map` senza tipi numerici espliciti. Per le proprietà semplici UDP conserva il valore JSON e registra il datatype dichiarato nel profilo. Il comparatore di identità `DECIMAL_V1` accetta la stessa rappresentazione.

**Date.** Dopo il trim, entrambi i profili richiedono una data di calendario valida con giorno e mese di due cifre e anno da 0001 a 9999. La conversione produce una data ISO locale. I metadati temporali del record seguono le rispettive policy.

**Celle vuote ed errori.** Il trim usa l'insieme Unicode `White_Space` esplicito di `ouf.value-types.v1` r2. Include NBSP e gli altri separatori elencati nel contratto ed esclude U+001C–U+001F e U+FEFF. Una cella `null` o una stringa vuota dopo il trim indica assenza. La pipeline omette la chiave dal record mappato e la prova di compatibilità accetta la riga. L’evidenza RAW conserva la cella originale. Un valore tipizzato non nullo di tipo diverso da stringa è un errore di dati. `IDENTITY` conserva stringa vuota, spazi e valori storici degli altri adapter, incluso `null`. Un campo sorgente mancante produce `ING_MAPPING_SOURCE_FIELD_MISSING`. Una regola sconosciuta produce `ING_TRANSFORM_UNSUPPORTED`.

Una conversione fallita blocca la prova di prontezza. La risposta HTTP 422 contiene `ING_TRANSFORM_VALUE_INVALID` e `rowOrdinal`, l’ordinale del record fornito dall’adapter (per il CSV, da 1 dopo l’intestazione). In esecuzione l’evidenza RAW conserva la cella originale. Il motivo di quarantena contiene il codice e il riferimento all’evidenza include l’ordinale. Il percorso predefinito mantiene il checkpoint e blocca la pubblicazione della riga.

Le composizioni possono usare campi inclusi nella proiezione anche quando il campo non ha una proprietà propria. I componenti seguono la definizione del `DATATYPE` fissata da `semanticId@publicationSetId`. Il gateway confronta la definizione e il digest con la risposta del Registry. La prova di prontezza richiede revisioni `ACTIVE`, incluse quelle annidate. La rilettura di una pubblicazione accetta anche definizioni `DEPRECATED` o `RETIRED`. L’envelope NORMALIZED punta al record RAW tramite `sourceDataRef`. Il lineage conserva il riferimento RAW e il suo SHA-256, così il valore sorgente resta recuperabile anche quando la rappresentazione canonica cambia.

**Geometrie WKT.** `WKT_V1` rifiuta le geometrie con un’ordinata di misura (`M` o `ZM`) con `ING_TRANSFORM_VALUE_INVALID`. La forma canonica è una geometria GeoJSON RFC 7946, in cui la terza ordinata rappresenta l’altitudine e la misura non ha una rappresentazione.

**Coordinate.** I lettori WKT e GeoJSON e il costruttore del punto convertono ogni coordinata al binary64 più vicino. Il dato canonico usa la forma più corta secondo ECMAScript. Nella riconsegna HTTP, candidato e predecessore rileggono lo stesso JSONB con la stessa codifica Jackson. Questa codifica può rappresentare lo stesso numero con una grafia diversa da ECMAScript, per esempio `1.0E-6` per `0.000001`. La prova `predecessor-return` usa coordinate sorgente oltre la precisione binary64 e confronta i byte dell'HTTP prima e dopo il ritorno.

**Value map.** Ingestion legge `valueMapRef`, `revisionId`, `targetVocabulary`, `definition` e `definitionSha256` dai binding semantici del bundle. La verifica a lotti usa le stesse regole dei `DATATYPE` e confronta il digest canonico della definizione incorporata con quello del Semantic. La ricaduta sul GET confronta anche la definizione. `VALUE_MAP_V1` cerca il codice dopo il trim nella tabella congelata, anche sui singoli elementi di `LIST_SPLIT_V1` e nei componenti strutturati. Un codice sconosciuto produce `ING_TRANSFORM_VALUE_INVALID` con `rowOrdinal`. Un pin privo della definizione produce `ING_VALUE_MAP_CONTENT_REQUIRED` prima della lettura dei dati. La definizione canonica può occupare fino a 786.432 byte UTF-8. Oltre 1.048.576 byte complessivi di definizioni nel bundle, Ingestion restituisce `ING_VALUE_MAP_LIMIT_EXCEEDED`. [Contratto delle value map](../contracts/ouf-value-maps-v1.json)

**Limiti di lettura.** La configurazione congelata, le letture di una singola pubblicazione (`active` e `resolve`) e ogni risposta di `discover` accettano fino a 3 MiB (3.145.728 byte di corpo HTTP). Le altre letture mantengono la soglia di 2 MiB (2.097.152 byte).

Ogni chiamata a `discover` parte da `limit=20`. Se la risposta supera 3 MiB, Ingestion interrompe la lettura e richiede una pagina più piccola dallo stesso cursore, dimezzando il limite da 20 a 10, 5, 2 e infine 1. Il client gestisce un corpo alla volta, verifica il numero di elementi rispetto al limite richiesto e usa il `nextAfter` della pagina valida per proseguire. Una pagina che supera la soglia anche con `limit=1` produce `ING_PUBLICATION_RESPONSE_TOO_LARGE`. Solo il superamento della soglia attiva il ridimensionamento della pagina.

**Verifica del contratto.** `OnboardingValueTransformContractTest` verifica lo SHA-256 dei due contratti e il catalogo dei lettori. Quando il ramo `main` di Onboarding contiene il contratto dei tipi, la CI confronta i byte delle due copie. Durante il rollout, per un checkout Onboarding precedente a D-084 rimane attivo il confronto con le cinque trasformazioni di Onboarding.
