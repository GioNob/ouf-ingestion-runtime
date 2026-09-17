# R2e — adapter GeoPackage

Autorità: Reality Baseline Package v1.7; letti tutti i sette PET e i documenti L0. [Regola obbligatoria di avvio sprint](https://github.com/GioNob/ouf-semantic-registry/blob/main/docs/OUF_SPRINT_PET_ALIGNMENT.md).

L'adapter `managed-geopackage-v1` esegue `INTERNAL_MANAGED_GEOPACKAGE` solo da bundle approvato e verificato. `layer`, `sourceCrs` e `geometryColumn` devono coincidere fra runtime approvato e execution profile. Servono `expectedSize`, `expectedHash`, `maxRows`, `maxCellChars` e chiavi `identityFields` non vuote, derivanti dalla source-object identity policy. Sono vietati ordinali e geometria come chiave.

Per ogni feature emette un SourceRecord, con attributi null preservati ed envelope geometrico `{crs, geoJson}`. L'identità usa sourceId, layer, nomi/valori delle chiavi con codifica a lunghezza e SHA-256; i valori sono trimmed. Non dipende da hash del file, ordine fisico o fid, salvo che il fid sia la chiave scelta esplicitamente. La chiave deve essere stabile nel sistema sorgente: la sola unicità nel campione non lo dimostra. Chiavi mancanti o duplicate rigettano l'apertura prima dell'emissione del primo record.

La provenienza include riferimento/checksum dell'asset, layer, fid, colonna geometrica e CRS. Il checkpoint usa l'ordinale nella lettura deterministica per primary key sullo stesso asset immutabile. Il RAW per feature, il NORMALIZED con provenienza e il CURATED seguono il percorso Lake esistente. La risoluzione canonica e i target delle relazioni appartengono a UDP.

## Perimetro

File fino a 10 MiB; 64 layer; 256 colonne; 10.000 feature per layer; geometria binaria fino a 1 MiB; budget 100.000 elementi/coordinate e profondità 16. SQLite read-only/immutable, estensioni disabilitate, trusted_schema disabilitato, tabelle effettive e identificatori quoted; handler VM con scadenza di 5 secondi. Il file temporaneo viene eliminato alla chiusura.

Supportati Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon e GeometryCollection 2D, CRS EPSG esplicito. Geometrie vuote, Z/M, curve e codifiche estese non vengono appiattite: sono rigettate. Non si indovina il CRS, non si riparano geometrie e non si riproietta nell'adapter. Altri formati/casi GIS restano R4b.

SQLite JDBC è fissato a 3.53.4.0, dalla [release upstream](https://github.com/xerial/sqlite-jdbc/releases/tag/3.53.4.0), e incluso nel packaging/scanning del modulo. Il reader è lo stesso di Onboarding; la CI R2e ne verifica l'identità per evitare divergenze fra profilazione ed esecuzione.

## Verifica

I test eseguono veri file SQLite/GeoPackage con due layer, ricaricamento con fid/ordine/geometria/referenza variati, checkpoint e null. I casi negativi comprendono header/WKB/SRID/dimensioni, limiti, layer, chiavi e checksum non validi.

`pairwise/r2e_geopackage.py` verifica approvazione, pubblicazione, acquisizione, serving, relazione tardiva/inversa, reload senza duplicati, storico e autorizzazioni su quattro processi owner, PostGIS e MinIO. Gateway/identità sono fixture dichiarate; non certifica browser, IAM/Gateway operativi o grigliati IGM reali.
