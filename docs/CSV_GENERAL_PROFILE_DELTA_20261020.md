# CSV general profile — WP5 incremental delta

Mini-PET v1.1 baseline d190193773116c9a59403aa42ccc9f89423eb959 remains unchanged. Normative basis actually consulted: Ingestion PET1.3 §§9.1,10–11, Onboarding PET1.6 §§7.3–7.6,8–9. No target execution or acceptance is implied.

The CSV adapter rejects malformed UTF-8, blank/duplicate/over-limit headers and inconsistent row widths before returning a cursor. BOM, explicit comma/semicolon, quoted newlines and empty lexical cells remain supported. CSV-AT03 module evidence: run39422161887, 191 tests, zero failures/errors, two conditional skips; the eleven adapter cases all executed. All eighteen push/PR workflow runs on506e78207ad68bf4b2ac1234d65b5810fe98121d passed, including consumer and source-continuity probes.

## Declared conversions

Existing propertyMappings.transform remains the versioned rule identifier. No new URL, implicit locale, inferred datatype, schema migration or dynamic expression is introduced.

| Rule | Input | Output |
|---|---|---|
| IDENTITY | Historical supported value, including null | Same value |
| DECIMAL_DOT_V1 | ASCII decimal, optional minus, explicit dot | BigDecimal |
| DECIMAL_COMMA_V1 | ASCII decimal, optional minus, explicit comma | BigDecimal |
| DATE_ISO_V1 | Strict uuuu-MM-dd | ISO local date string |
| DATE_DMY_V1 | Strict dd/MM/uuuu | ISO local date string |

Decimal profiles accept no grouping, whitespace, exponent, plus sign or leading zeroes beyond zero itself. Maximum256 input characters,38 significant/integer digits and18 nonzero fractional places after trailing-zero removal. Dates require real calendar dates in years0001–9999. They do not assign observedAt, timezone, validFrom or validTo. Invalid values are permanent ING_DATATYPE_COERCION_FAILED; unknown rules remain ING_TRANSFORM_UNSUPPORTED. These finite profiles can be extended only through an explicit versioned contract, never silent guessing.

Compatibility and execution call the same conversion code. RAW evidence preserves the original values; lineage retains rawObjectRef, mappingRefs and exact transformationIds. Tests cover pure conversions, unchanged IDENTITY, compatibility without persistence, actual outbox numeric/date payload and raw/lineage evidence, and invalid-value quarantine without outbox/checkpoint advance.

## Remaining gates

Typed conversion CI is required. Onboarding proposal/validation, UDP datatype aliases, published semantic bindings, vocabulary value-map ownership and simple relationship qualification remain open. A compile or module CI PASS cannot authorize activation. No new candidate image replaces0fe0de60c6d980fe97610acf195d4bfd46d3777f in the retained G2 rollout fixture or target packet. Integration belongs to G5/G7 after the relevant consumer contracts and runtime gates.
