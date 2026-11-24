package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;
import org.apache.poi.ss.usermodel.FormulaError;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class XlsxContractVectorsTest {
  static Stream<Arguments> vectors() throws IOException {
    try (var in = XlsxContractVectorsTest.class.getResourceAsStream("/contracts/xlsx-cell-text-v1.json")) {
      JsonNode root = new ObjectMapper().readTree(Objects.requireNonNull(in));
      assertThat(root.required("version").asText()).isEqualTo("1.1.0");
      assertThat(root.required("cells").size()).isEqualTo(47);
      assertThat(root.required("sheets").size()).isEqualTo(11);
      var ids = new HashSet<String>();
      var cases = Stream.<Arguments>builder();
      for (String kind : List.of("cells", "sheets")) {
        for (var vector : root.required(kind)) {
          String id = vector.required("id").asText();
          assertThat(ids.add(id)).as(id).isTrue();
          cases.add(arguments(id, kind, vector, root.required("defaults")));
        }
      }
      return cases.build();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("vectors")
  void sharedVector(String id, String kind, JsonNode vector, JsonNode defaults) throws Exception {
    boolean cells = kind.equals("cells");
    var expected = vector.required("expect");
    String dateSystem = vector.path("dateSystem").asText(defaults.required("dateSystem").asText());
    JsonNode timeZone = vector.has("timeZone") ? vector.get("timeZone") : defaults.required("timeZone");
    String zone = timeZone.isNull() ? null : timeZone.asText();
    int header = vector.path("headerRow").asInt(expected.path("headerRow").asInt(1));
    byte[] bytes;
    try (var wb = new XSSFWorkbook()) {
      wb.getCTWorkbook().getWorkbookPr().setDate1904(dateSystem.equals("1904"));
      var sheet = wb.createSheet("Approved");
      if (cells) {
        sheet.createRow(0).createCell(0).setCellValue("v");
        var cell = sheet.createRow(1).createCell(0);
        var spec = vector.required("cell");
        setCell(cell, spec);
        if (spec.has("numberFormat")) {
          var style = wb.createCellStyle();
          style.setDataFormat(wb.createDataFormat().getFormat(spec.get("numberFormat").asText()));
          cell.setCellStyle(style);
        }
      } else {
        vector.required("rows").fields().forEachRemaining(entry -> {
          var row = sheet.createRow(Integer.parseInt(entry.getKey()) - 1);
          int column = 0;
          for (var value : entry.getValue()) row.createCell(column++).setCellValue(value.asText());
        });
        for (var area : vector.path("merged")) sheet.addMergedRegion(CellRangeAddress.valueOf(area.asText()));
        if (vector.path("hidden").asBoolean()) {
          wb.createSheet("Visible");
          wb.setSheetHidden(0, true);
        }
      }
      bytes = serialize(wb);
    }
    var params = parameters("Approved", 0, header, dateSystem, zone);
    var bundle = bundle(bytes, params, Map.of());
    var adapter = new ManagedXlsxAdapter((ref, size, hash) -> bytes);
    if (expected.has("reject")) {
      var error = catchThrowableOfType(() -> adapter.open(bundle, null), AdapterSpi.AdapterException.class);
      assertThat(error.code()).isEqualTo("ING_MANAGED_" + expected.required("reject").asText());
      assertThat(error.errorClass()).isEqualTo(AdapterSpi.ErrorClass.DATA);
      if (expected.has("at")) assertThat(error.getMessage()).contains(expected.get("at").asText());
      return;
    }
    try (var cursor = adapter.open(bundle, null)) {
      if (cells) {
        String text = expected.required("text").asText();
        // An empty lexical value still has a vector, even though its entire record is skipped.
        try (var reopened = new XSSFWorkbook(new ByteArrayInputStream(bytes))) {
          assertThat(XlsxCellText.read(reopened.getSheetAt(0).getRow(1).getCell(0), reopened.isDate1904(),
              zone == null ? null : ZoneId.of(zone))).isEqualTo(text);
        }
        if (!text.isEmpty()) assertThat(cursor.next().orElseThrow().payload()).containsExactly(entry("v", text));
      } else {
        int count = 0;
        while (cursor.next().isPresent()) count++;
        assertThat(count).isEqualTo(expected.required("recordCount").asInt());
      }
      assertThat(cursor.next()).isEmpty();
    }
  }

  private static void setCell(XSSFCell cell, JsonNode spec) {
    JsonNode value = spec.path("value");
    switch (spec.required("type").asText()) {
      case "BLANK" -> cell.setBlank();
      case "STRING" -> cell.setCellValue(value.asText());
      case "NUMERIC" -> cell.setCellValue(value.doubleValue());
      case "BOOLEAN" -> cell.setCellValue(value.booleanValue());
      case "ERROR" -> cell.setCellErrorValue(FormulaError.forString(value.asText()).getCode());
      case "FORMULA" -> {
        cell.setCellFormula(spec.required("formula").asText());
        if (!spec.required("cached").isNull()) setCell(cell, spec.get("cached"));
      }
      default -> throw new AssertionError(spec.toString());
    }
  }

  static byte[] serialize(XSSFWorkbook wb) throws IOException {
    var out = new ByteArrayOutputStream();
    wb.write(out);
    return out.toByteArray();
  }

  static Map<String,Object> parameters(String sheet, int index, int header, String system, String zone) {
    var xlsx = new LinkedHashMap<String,Object>(Map.of("sheet", sheet, "sheetIndex", index, "headerRow", header,
        "dateSystem", system, "cellText", "ouf.xlsx-cell-text.v1"));
    if (zone != null) xlsx.put("timeZone", zone);
    return Map.of("xlsx", xlsx);
  }

  static ExecutionBundle bundle(byte[] bytes, Map<String,Object> parameters, Map<String,Object> extra) {
    var c = new LinkedHashMap<String,Object>(Map.of("readParameters", parameters,
        "expectedHash", ExecutionGatewayClient.hash(bytes), "expectedSize", bytes.length,
        "maxRows", 100, "maxColumns", 100, "maxCellChars", 1000, "adapterId", ManagedXlsxAdapter.ID));
    c.putAll(extra);
    return new ExecutionBundle("bundle", "1", "sha256:pinned", "source", ManagedXlsxAdapter.MODE, "object://asset", c);
  }
}
