# Qualificazione base R4a preservata — 10 ottobre 2026

Base 163c167d09b8371ff7a62ce7068e9d485b6969b7: conserva IAM, compatibilità della configurazione congelata e retry governato già corretti sul branch avanzato. Non è un deploy del main semplificato. Migrazioni V1–V14, sorgente SDK e contratti restano byte-identici.

Riusa la qualificazione OSS di main 3a33c6c248ec3a1151f5a4cb888a2b9598b16418: Boot4.0.8, Jackson2.21.7 transitorio, modulo Flyway, server/metriche BOM compatibili, health package aggiornato. Il Resource Server IAM viene conservato con starter Boot4 e i test del boundary HTTP mantengono le asserzioni positive/negative; nessun bypass IAM. Scanner HIGH/CRITICAL include non corretti senza soppressioni.

Prima del rilascio: CI sul candidato esatto, regressioni automatiche/R2 e IAM, compatibilità N/N+1, inventory delle migrazioni, backup/restore e package/attestazione target. Non inferire il live dalla CI e non replay Cinema/Teatri/claim consumati. No mutazione VPS.

## Base runtime e gate più completo

La scansione senza ignore-unfixed del run38068296861 segnala 9 HIGH non corretti nel catalogo per libexpat1/libuuid1 della base distroless Debian13. Non è un difetto Java e non viene soppresso. La qualificazione passa alla base Temurin21 JRE Alpine già usata da UDP, con aggiornamento pacchetti e UID/GID10002 invariati. Una prova CI esegue SQLite JDBC nativo e round-trip XLSX/DataFormatter nell'immagine finale non-root, filesystem readonly, rete disabilitata e tmpfs privata; nessuna formula eseguita. Tutti gli R2b/c/e, IAM e recovery rimangono richiesti. La fixture è esclusa dal JAR di produzione. Nessuna nuova immagine importata sul VPS; il precedente scan fallito resta evidenza.

La ricetta usa ancora un tag upstream: l'artefatto di rilascio deve fissare digest e provenance del build effettivo. Questa prova non certifica build bit-for-bit o tutte le geometrie/formati.
