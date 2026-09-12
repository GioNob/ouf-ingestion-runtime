# Required follow-up after Ingestion Runtime

This is a tracked cross-module follow-up, not Ingestion Runtime ownership.

After completing Ingestion Runtime, return to `ouf-source-onboarding` and implement:

1. separate per-field extraction selection from per-field data-access labels (for example public/private/excluded), publishing property-scoped `DataAccessPolicy` references while Authorization and downstream modules retain enforcement ownership;
2. persisted, version-pinned controlled-vocabulary mappings (`vocabularyId`, `vocabularyVersion`, `valueMapRef`) backed by Semantic Registry search/validation without duplicating Registry authoring;
3. trusted actor, tenant, Authorization-decision reference, correlation and append-only audit evidence for incremental mapping mutations, without forcing ordinary schema-only mapping through the Trusted Human Surface;
4. a strictly typed `SCHEMA_ONLY` discovery response that cannot carry instance samples, plus an explicitly governed and separately authorized profiling/sample mode;
5. MCP-ready incremental and idempotent capabilities so the chatbot can drive the UX. The chatbot/MCP Server owns presentation and conversational orchestration; Onboarding owns the governed commands and state.

THS remains reserved for policy-required approval/activation, ambiguity, quarantine, remediation and protected-log operations. Ontology and controlled-vocabulary publication remains owned by Semantic Registry.
