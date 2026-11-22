# Native semantic reference capacity

The governed file channel accepts up to 32 exact semantic references per published
configuration. Native class and property artifacts have separate revision and
publication identifiers: a class plus twenty properties needs 21 bindings. A
containing published ontology may instead need one binding, but an arbitrary
wrapper must not be invented to conceal independent revisions.

Ingestion's frozen compatibility probe and admission client now use the same
32-reference bound as the MCP file channel and UDP historical resolver. Each
binding still resolves its exact semantic ID, version, revision and publication;
missing, duplicate or mismatched references are denied. No frozen RC3 schema,
publication-page size, authorization decision or HUMAN gate changes. PET
Ingestion v1.3 requires published mappings and historical pins; it does not
prescribe the former implementation limit of twenty semantic references.

The HTTP regression covers 21 and 32 resolved references, rejection of 33 before
semantic requests, and a mismatch in the final reference. The compatibility
probe separately exercises the same capacity with the actual CSV parser and
canonical pipeline. These are component tests, not target CSV acceptance.

Requests retain the existing 2 MiB response and five-second complete-exchange
bounds. Slow resolution does not extend worker leases or authorize a stale
publication; existing claim fences remain mandatory. This change introduces no
new retry session and does not establish a target performance SLO.
