# R3 summary health versus catch-up window

MCP v1.4 §33, Ingestion v1.3 §46 and Matrix v1.7 require missing or bounded
information not to become false HEALTHY. The Reality Baseline v1.7 archive
passed all 323 checksum checks; the seven PET scopes and L0 boundaries were
consulted for this increment.

Previously summary counted OPEN only in its limited, time-filtered page.
A resolved recent incident could hide an older unresolved failure. Summary
now reads the requested catch-up page separately from a bounded current-state
scan (100 candidates). Both pass through the same owner resource/detail
policy. A full scan/page or redaction is explicitly partial; when no visible
OPEN exists but coverage is incomplete the status is UNKNOWN. Counts are
omitted unless coverage is complete. A visible OPEN remains DEGRADED.

The new HTTP-level test uses the real policy adapter with a source-scoped
TENANT_OPERATIONAL grant and verifies an old OPEN behind a recent one-item
RESOLVED page. Database access in this test is stubbed; module CI supplies the
existing database checks. No migration, scheduler change or grant publication.

This does not complete persistent incident lifecycle, exact total pagination,
30-day deployment retention or live installation acceptance. The receipt/SDK adapter is now implemented;
see R3_PRODUCER_IDENTITY.md. Deployment and actual policy refresh stay
explicit R3 integration/acceptance gates. See the companion MCP and Gateway
R3_SUMMARY_GOVERNED.md records for the execution topology and deployment gates.
