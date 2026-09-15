package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;

class FrozenContractValidatorTest {
  private final FrozenContractValidator validator=new FrozenContractValidator(new ObjectMapper());
  @Test void acceptsMinimalValidEnvelopeAndRejectsMissingRequiredField(){
    Map<String,Object> valid=new LinkedHashMap<>(Map.ofEntries(Map.entry("envelopeVersion","1.0"),Map.entry("ingestionId","i"),Map.entry("runId","r"),Map.entry("sourceId","s"),Map.entry("sourceSchemaId","schema"),Map.entry("sourceSchemaVersion","1"),Map.entry("configurationBundleId","b"),Map.entry("configurationBundleVersion","1"),Map.entry("operation","UPSERT"),Map.entry("observedAt","2026-01-01T00:00:00Z"),Map.entry("acquiredAt","2026-01-01T00:00:00Z"),Map.entry("contentType","application/json"),Map.entry("provenance",Map.of("correlationId","c"))));
    validator.validate(Path.of("contracts/rc3/canonical-data-envelope-v1.json"),valid);
    valid.remove("sourceId");
    assertThatThrownBy(()->validator.validate(Path.of("contracts/rc3/canonical-data-envelope-v1.json"),valid)).isInstanceOf(FrozenContractValidator.ContractViolation.class).extracting("paths").asList().contains("$.sourceId");
  }
  @Test void enforcesConditionalHandoffRequirement(){
    Map<String,Object> handoff=new LinkedHashMap<>(Map.of("handoffId","h","ingestionRunId","r","ingestionId","i","sourceIdentity",Map.of("sourceId","s","typeCode","t","sourceObjectId","o"),"operation","UPSERT","contractRefs",Map.of("sourceSchemaRef","s:1","bundleRef","b:1","semanticPublicationSetRef","p:1","adapterProfileRef","a:1"),"lineageId","l","contentHash","sha256:x","acquiredAt","2026-01-01T00:00:00Z","changeRepresentation",Map.of("mode","FULL_SNAPSHOT")));
    assertThatThrownBy(()->validator.validate(Path.of("contracts/rc3/handoff-payload-v1.json"),handoff)).isInstanceOf(FrozenContractValidator.ContractViolation.class);
  }
  @Test void resolvesCommonLabelReferenceFromClasspathAndRejectsMalformedLabel(){
    Map<String,Object> handoff=new LinkedHashMap<>(Map.ofEntries(Map.entry("handoffId","h"),Map.entry("ingestionRunId","r"),Map.entry("ingestionId","i"),Map.entry("sourceIdentity",Map.of("sourceId","s","typeCode","t","sourceObjectId","o")),Map.entry("operation","UPSERT"),Map.entry("canonicalPayload",Map.of()),Map.entry("contractRefs",Map.of("sourceSchemaRef","s:1","bundleRef","b:1","semanticPublicationSetRef","p:1","adapterProfileRef","a:1")),Map.entry("lineageId","l"),Map.entry("contentHash","sha256:x"),Map.entry("acquiredAt","2026-01-01T00:00:00Z"),Map.entry("changeRepresentation",Map.of("mode","FULL_SNAPSHOT")),Map.entry("dataAccessLabels",List.of(Map.of("labelId","PUBLIC","version","1")))));
    validator.validate("/contracts/rc3/handoff-payload-v1.json",handoff);
    handoff.put("dataAccessLabels",List.of(Map.of("version","1")));
    assertThatThrownBy(()->validator.validate("/contracts/rc3/handoff-payload-v1.json",handoff)).isInstanceOf(FrozenContractValidator.ContractViolation.class).extracting("paths").asList().contains("$.dataAccessLabels[0].labelId");
  }
}
