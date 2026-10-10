# Qualificazione base R4a preservata — 10 ottobre 2026

Base 163c167d09b8371ff7a62ce7068e9d485b6969b7: conserva IAM, compatibilità della configurazione congelata e retry governato già corretti sul branch avanzato. Non è un deploy del main semplificato. Migrazioni V1–V14, sorgente SDK e contratti restano byte-identici.

Riusa la qualificazione OSS di main 3a33c6c248ec3a1151f5a4cb888a2b9598b16418: Boot4.0.8, Jackson2.21.7 transitorio, modulo Flyway, server/metriche BOM compatibili, health package aggiornato. Il Resource Server IAM viene conservato con starter Boot4 e i test del boundary HTTP mantengono le asserzioni positive/negative; nessun bypass IAM. Scanner HIGH/CRITICAL include non corretti senza soppressioni.

Prima del rilascio: CI sul candidato esatto, regressioni automatiche/R2 e IAM, compatibilità N/N+1, inventory delle migrazioni, backup/restore e package/attestazione target. Non inferire il live dalla CI e non replay Cinema/Teatri/claim consumati. No mutazione VPS.
