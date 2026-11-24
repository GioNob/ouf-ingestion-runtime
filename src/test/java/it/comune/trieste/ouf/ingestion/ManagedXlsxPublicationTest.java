package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.PublishedActivation.map;
import static it.comune.trieste.ouf.ingestion.XlsxContractVectorsTest.parameters;
import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.junit.jupiter.api.Test;

class ManagedXlsxPublicationTest {
  private final ObjectMapper json = new ObjectMapper();
  private final Map<String,Object> candidate = new FrozenConfigurationProbeTest().candidate();
  private final Map<String,Object> config = map(candidate, "configuration");
  private final Map<String,Object> runtime = map(map(config, "extractionProfile"), "runtime");
  private final Map<String,Object> execution = map(runtime, "execution");

  private Map<String,Object> xlsx() {
    var parameters = parameters("Approved", 0, 1, "1900", null);
    runtime.put("readParameters", parameters);
    execution.putAll(Map.of("acquisitionMode", "INTERNAL_MANAGED_XLSX", "adapterId", "managed-xlsx-v1",
        "adapterProfileRef", "adapter://managed-xlsx/1", "readParameters", parameters));
    return parameters;
  }

  private ExecutionBundle project() {
    return PublishedExecutionMapper.map(new ExecutionBundle("bundle", "1", "sha256:pinned", "source", "MANAGED",
        null, Map.of("publishedBundle", config)));
  }

  @Test void mapperPinsOnlyTheApprovedSheetParameters() {
    var approved = xlsx();
    assertThat(project().configuration()).containsEntry("readParameters", approved);
    for (String key : List.of("sheet", "sheetIndex", "headerRow", "dateSystem", "cellText", "timeZone")) {
      var changed = new LinkedHashMap<>(map(approved, "xlsx"));
      changed.put(key, switch (key) {
        case "sheet" -> "Other";
        case "sheetIndex", "headerRow" -> 2;
        case "dateSystem" -> "1904";
        case "cellText" -> "v2";
        default -> "Europe/Rome";
      });
      execution.put("readParameters", Map.of("xlsx", changed));
      assertThatThrownBy(this::project).as(key).hasMessage("ING_EXECUTION_PROFILE_INVALID");
    }
    execution.put("readParameters", approved);
    runtime.remove("readParameters");
    assertThatThrownBy(this::project).hasMessage("ING_EXECUTION_PROFILE_INVALID");
    runtime.put("readParameters", approved);
    execution.remove("readParameters");
    execution.put("sheet", "Approved");
    assertThatThrownBy(this::project).hasMessage("ING_EXECUTION_PROFILE_INVALID");
  }

  @Test void identicalButInvalidParametersFailClosed() {
    var good = map(xlsx(), "xlsx");
    var bad = new ArrayList<Map<String,Object>>();
    for (String key : good.keySet()) {
      var missing = new LinkedHashMap<>(good);
      missing.remove(key);
      bad.add(missing);
    }
    Map<String,Object> invalid = Map.of("sheetIndex", -1, "headerRow", "1", "timeZone", "Invalid/Zone",
        "cellText", "v2", "dateSystem", "1901", "sheet", "", "unknown", true);
    invalid.forEach((key, value) -> {
      var changed = new LinkedHashMap<>(good);
      changed.put(key, value);
      bad.add(changed);
    });
    for (var params : bad) {
      runtime.put("readParameters", Map.of("xlsx", params));
      execution.put("readParameters", runtime.get("readParameters"));
      assertThatThrownBy(this::project).as(params.toString()).hasMessage("ING_EXECUTION_PROFILE_INVALID");
    }
  }

  @Test void frozenProbeReadsApprovedWorkbookWithOrdinalIdentity() throws Exception {
    byte[] bytes = ManagedXlsxAdapterTest.workbook(wb -> {
      var s = wb.createSheet("Approved");
      var h = s.createRow(0);
      h.createCell(0).setCellValue("id");
      h.createCell(1).setCellValue("name");
      var row = s.createRow(4);
      row.createCell(0).setCellValue("007");
      var cell = row.createCell(1);
      cell.setCellFormula("1+1");
      cell.setCellValue(2);
    });
    xlsx();
    execution.put("expectedSize", bytes.length);
    runtime.put("contentHash", ExecutionGatewayClient.hash(bytes));
    runtime.put("rowIdentityBasis", "ASSET_AND_ROW_ORDINAL");
    config.put("sourceObjectIdentityPolicy", Map.of("strategy", "MANAGED_DETERMINISTIC",
        "sourceFields", List.of("$managedRowOrdinal"), "normalizationRuleRef", "normalization://managed-file/asset-row-ordinal-v1"));
    candidate.put("configurationHash", FrozenConfigurationProbe.configurationHash(json, config));
    var probe = new FrozenConfigurationProbe(json, (r,s,h) -> bytes, refs -> assertThat(refs).containsExactly("places@1"));
    assertThat(probe.probe(candidate)).containsEntry("status", "PASS").containsEntry("validatedRows", 1L)
        .containsEntry("adapterId", "managed-xlsx-v1");
  }

  @Test void routesLegacyXlsxAndCsvThroughTheOriginalAdapter() {
    RuntimePorts.ManagedObjectPort objects = (r,s,h) -> { throw new AssertionError("Unexpected read"); };
    for (String mode : List.of("INTERNAL_MANAGED_CSV", "INTERNAL_MANAGED_XLSX")) {
      execution.put("acquisitionMode", mode);
      assertThat(ExecutionRuntimeConfiguration.managedAdapter(project(), objects, "unsupported"))
          .isExactlyInstanceOf(ManagedTabularAdapter.class);
    }
    xlsx();
    assertThat(ExecutionRuntimeConfiguration.managedAdapter(project(), objects, "unsupported"))
        .isExactlyInstanceOf(ManagedXlsxAdapter.class);
    execution.put("acquisitionMode", "INTERNAL_MANAGED_CSV");
    assertThatThrownBy(this::project).hasMessage("ING_EXECUTION_PROFILE_INVALID");
  }
}
