package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.XlsxContractVectorsTest.*;
import static org.assertj.core.api.Assertions.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.zip.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.crypt.*;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.xssf.usermodel.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ManagedXlsxAdapterTest {
  private static final Map<String,Object> APPROVED = parameters("Approved", 0, 1, "1900", null);

  static byte[] workbook(Consumer<XSSFWorkbook> build) throws IOException {
    try (var wb = new XSSFWorkbook()) {
      build.accept(wb);
      return serialize(wb);
    }
  }

  static byte[] table() throws IOException {
    return workbook(wb -> {
      var sheet = wb.createSheet("Approved");
      var header = sheet.createRow(0);
      header.createCell(0).setCellValue("id");
      header.createCell(1).setCellValue("name");
      header.createCell(8).setBlank();
      sheet.createRow(2).createCell(0).setCellValue(42);
      var last = sheet.createRow(7);
      last.createCell(0).setCellValue("007");
      last.createCell(1).setCellValue(true);
    });
  }

  private static ManagedXlsxAdapter reader(byte[] bytes) {
    return new ManagedXlsxAdapter((ref, size, hash) -> bytes);
  }

  private static void rejection(AdapterSpi adapter, ExecutionBundle bundle, String code, AdapterSpi.ErrorClass kind) {
    var e = catchThrowableOfType(() -> adapter.open(bundle, null), AdapterSpi.AdapterException.class);
    assertThat(e.code()).isEqualTo(code);
    assertThat(e.errorClass()).isEqualTo(kind);
  }

  @Test void resumesOnlyAtAnEmittedPhysicalOrdinal() throws Exception {
    var bytes = table();
    var bundle = bundle(bytes, APPROVED, Map.of("sheet", "ignored"));
    var reader = reader(bytes);
    AdapterSpi.Checkpoint checkpoint;
    try (var first = reader.open(bundle, null)) {
      assertThat(first.checkpoint().value()).containsEntry("ordinal", 0L);
      var row = first.next().orElseThrow();
      assertThat(row.ordinal()).isEqualTo(2);
      assertThat(row.payload()).containsEntry("id", "42").containsEntry("name", null);
      assertThat(row.provenance()).containsExactlyInAnyOrderEntriesOf(Map.of("managedObjectRef", "object://asset",
          "sheet", "Approved", "rowOrdinal", 2L));
      assertThat(row.sourceObjectId()).isEqualTo(ManagedIdentity.sourceObjectId("source", ExecutionGatewayClient.hash(bytes),
          "Approved", 2, row.payload(), List.of()));
      checkpoint = first.checkpoint();
      assertThat(checkpoint.value()).containsEntry("ordinal", 2L);
    }
    try (var resumed = reader.open(bundle, checkpoint)) {
      var last = resumed.next().orElseThrow();
      assertThat(last.ordinal()).isEqualTo(7);
      assertThat(last.payload()).containsExactly(entry("id", "007"), entry("name", "true"));
      assertThat(resumed.next()).isEmpty();
      try (var exhausted = reader.open(bundle, resumed.checkpoint())) { assertThat(exhausted.next()).isEmpty(); }
    }
    for (int ordinal : List.of(-1, 3, 8)) {
      assertThatThrownBy(() -> reader.open(bundle, new AdapterSpi.Checkpoint(Map.of("ordinal", ordinal))))
          .hasMessage("ING_CHECKPOINT_OUT_OF_RANGE");
    }
  }

  @Test void checksBothWorkbookCoordinatesAndDateSystem() throws Exception {
    byte[] bytes = workbook(wb -> {
      wb.createSheet("Other").createRow(0).createCell(0).setCellFormula("1/0");
      wb.createSheet(" Approved ").createRow(0).createCell(0).setCellValue("v");
    });
    var reader = reader(bytes);
    for (var p : List.of(parameters(" Approved ", 0, 1, "1900", null), parameters(" approved ", 1, 1, "1900", null),
        parameters("Approved", 1, 1, "1900", null), parameters(" Approved ", 1, 1, "1904", null))) {
      rejection(reader, bundle(bytes, p, Map.of()), "ING_MANAGED_XLSX_PARAMETERS_MISMATCH", AdapterSpi.ErrorClass.CONFIGURATION);
    }
    try (var cursor = reader.open(bundle(bytes, parameters(" Approved ", 1, 1, "1900", null), Map.of()), null)) {
      assertThat(cursor.next()).isEmpty();
    }
  }

  @Test void enforcesExecutionLimitsAtTheBoundaryAndOneBeyond() throws Exception {
    byte[] bytes = table();
    var reader = reader(bytes);
    var exact = Map.<String,Object>of("maxRows", 2, "maxColumns", 2, "maxCellChars", 4);
    try (var cursor = reader.open(bundle(bytes, APPROVED, exact), null)) {
      assertThat(cursor.next()).isPresent();
      assertThat(cursor.next()).isPresent();
      assertThat(cursor.next()).isEmpty();
    }
    rejection(reader, bundle(bytes, APPROVED, Map.of("maxRows", 1)), "ING_MANAGED_XLSX_ROW_LIMIT", AdapterSpi.ErrorClass.DATA);
    rejection(reader, bundle(bytes, APPROVED, Map.of("maxColumns", 1)), "ING_MANAGED_XLSX_COLUMN_LIMIT", AdapterSpi.ErrorClass.DATA);
    rejection(reader, bundle(bytes, APPROVED, Map.of("maxCellChars", 3)), "ING_MANAGED_CELL_TOO_LARGE", AdapterSpi.ErrorClass.DATA);
  }

  @Test void chartSheetKeepsItsPhysicalSlotButIsNotAWorksheet() throws Exception {
    byte[] original = workbook(wb -> {
      wb.createSheet("Chart");
      wb.createSheet("Approved").createRow(0).createCell(0).setCellValue("v");
    });
    var single = new ManagedXlsxAdapter.ArchiveLimits(1 << 20, 1 << 20, 1 << 20, .01, 100, 1);
    var approved = parameters("Approved", 1, 1, "1900", null);
    rejection(new ManagedXlsxAdapter((r,s,h) -> original, single), bundle(original, approved, Map.of()),
        "ING_MANAGED_XLSX_SHEET_LIMIT", AdapterSpi.ErrorClass.DATA);
    var parts = parts(original);
    for (String path : List.of("[Content_Types].xml", "xl/_rels/workbook.xml.rels")) {
      String xml = new String(parts.get(path), StandardCharsets.UTF_8);
      var match = java.util.regex.Pattern.compile("<[^<>]*worksheets/sheet1\\.xml[^<>]*>").matcher(xml);
      assertThat(match.find()).as(path).isTrue();
      String element = match.group();
      String chartElement = element.replace("worksheets/sheet1.xml", "chartsheets/sheet1.xml")
          .replace(".worksheet+xml", ".chartsheet+xml").replace("/worksheet\"", "/chartsheet\"");
      parts.put(path, xml.replace(element, chartElement).getBytes(StandardCharsets.UTF_8));
    }
    parts.remove("xl/worksheets/sheet1.xml");
    parts.put("xl/chartsheets/sheet1.xml",
        "<chartsheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"/>".getBytes(StandardCharsets.UTF_8));
    byte[] bytes = zip(parts);
    var reader = new ManagedXlsxAdapter((r,s,h) -> bytes, single);
    try (var cursor = reader.open(bundle(bytes, approved, Map.of()), null)) {
      assertThat(cursor.next()).isEmpty();
    }
    for (String sheet : List.of("Approved", "Chart")) {
      rejection(reader, bundle(bytes, parameters(sheet, 0, 1, "1900", null), Map.of()),
          "ING_MANAGED_XLSX_PARAMETERS_MISMATCH", AdapterSpi.ErrorClass.CONFIGURATION);
    }
  }

  @ParameterizedTest
  @ValueSource(doubles = {59, 2958465.999999, 2958466, 1e20})
  void rejectsDatesOutsideRoundedDateDomain(double serial) throws Exception {
    byte[] bytes = workbook(wb -> {
      var sheet = wb.createSheet("Approved");
      sheet.createRow(0).createCell(0).setCellValue("v");
      var cell = sheet.createRow(1).createCell(0);
      cell.setCellValue(serial);
      var style = wb.createCellStyle();
      style.setDataFormat(wb.createDataFormat().getFormat("yyyy-mm-dd"));
      cell.setCellStyle(style);
    });
    rejection(reader(bytes), bundle(bytes, APPROVED, Map.of()), "ING_MANAGED_XLSX_DATE_OUT_OF_RANGE", AdapterSpi.ErrorClass.DATA);
  }

  @Test void rejectsLateDuplicateIdentityBeforeReturningACursor() throws Exception {
    byte[] bytes = workbook(wb -> {
      var sheet = wb.createSheet("Approved");
      sheet.createRow(0).createCell(0).setCellValue("id");
      sheet.createRow(1).createCell(0).setCellValue(42);
      sheet.createRow(9).createCell(0).setCellValue("42");
    });
    rejection(reader(bytes), bundle(bytes, APPROVED, Map.of("identityFields", List.of("id"))),
        "ING_MANAGED_DUPLICATE_IDENTITY", AdapterSpi.ErrorClass.DATA);
  }

  @ParameterizedTest
  @ValueSource(strings = {"<v/>", "<v>bogus</v>", "<v>NaN</v>", "<v>1e999</v>", "boolean:2", "boolean:true", "<v>0</v>"})
  void validatesPersistedFormulaCacheXml(String replacement) throws Exception {
    byte[] original = workbook(wb -> {
      var sheet = wb.createSheet("Approved");
      sheet.createRow(0).createCell(0).setCellValue("v");
      var cell = sheet.createRow(1).createCell(0);
      cell.setCellFormula("6*7");
      cell.setCellValue(42);
    });
    var parts = parts(original);
    String path = "xl/worksheets/sheet1.xml";
    String xml = new String(parts.get(path), StandardCharsets.UTF_8);
    if (replacement.startsWith("boolean:")) {
      String numericCell = "<c\\b[^>]*\\br=\"A2\"[^>]*>";
      assertThat(xml).containsPattern(numericCell);
      xml = xml.replaceFirst(numericCell, "<c r=\"A2\" t=\"b\">");
      replacement = "<v>" + replacement.substring(8) + "</v>";
    }
    assertThat(xml).containsPattern("<v>42(?:\\.0)?</v>");
    parts.put(path, xml.replaceAll("<v>42(?:\\.0)?</v>", replacement).getBytes(StandardCharsets.UTF_8));
    byte[] bytes = zip(parts);
    var b = bundle(bytes, APPROVED, Map.of());
    if (replacement.equals("<v>0</v>")) {
      try (var cursor = reader(bytes).open(b, null)) {
        assertThat(cursor.next().orElseThrow().payload()).containsEntry("v", "0");
      }
    } else {
      rejection(reader(bytes), b, "ING_MANAGED_XLSX_FORMULA_WITHOUT_CACHED_VALUE", AdapterSpi.ErrorClass.DATA);
    }
  }

  @Test void enforcesInstallationArchiveBudgetsOnActualBytes() throws Exception {
    byte[] bytes = table();
    var parts = parts(bytes);
    int total = parts.values().stream().mapToInt(b -> b.length).sum();
    int largest = parts.values().stream().mapToInt(b -> b.length).max().orElseThrow();
    int entries = parts.size();
    var exact = new ManagedXlsxAdapter.ArchiveLimits(bytes.length, total, largest, .01, entries, 1);
    var bundle = bundle(bytes, APPROVED, Map.of());
    try (var cursor = new ManagedXlsxAdapter((r,s,h) -> bytes, exact).open(bundle, null)) {
      assertThat(cursor.next()).isPresent();
    }
    var cases = Map.of(
        new ManagedXlsxAdapter.ArchiveLimits(bytes.length - 1, total, largest, .01, entries, 1), "FILE_LIMIT",
        new ManagedXlsxAdapter.ArchiveLimits(bytes.length, total - 1, largest, .01, entries, 1), "INFLATED_SIZE_LIMIT",
        new ManagedXlsxAdapter.ArchiveLimits(bytes.length, total, largest - 1, .01, entries, 1), "INFLATED_SIZE_LIMIT",
        new ManagedXlsxAdapter.ArchiveLimits(bytes.length, total, largest, .01, entries - 1, 1), "ENTRY_COUNT_LIMIT",
        new ManagedXlsxAdapter.ArchiveLimits(bytes.length, total, largest, 1, entries, 1), "INFLATE_RATIO_LIMIT");
    cases.forEach((limits, suffix) -> rejection(new ManagedXlsxAdapter((r,s,h) -> bytes, limits), bundle,
        "ING_MANAGED_XLSX_" + suffix, AdapterSpi.ErrorClass.DATA));
  }

  @Test void rejectsXlsMacrosAndEncryptedWorkbooksByContent() throws Exception {
    var ole = new ByteArrayOutputStream();
    try (var wb = new HSSFWorkbook()) { wb.createSheet(); wb.write(ole); }
    var encrypted = new ByteArrayOutputStream();
    try (var fs = new POIFSFileSystem()) {
      var enc = new EncryptionInfo(EncryptionMode.agile).getEncryptor();
      enc.confirmPassword("synthetic-fixture");
      try (var out = enc.getDataStream(fs)) { out.write(table()); }
      fs.writeFilesystem(encrypted);
    }
    byte[] macro;
    try (var wb = new XSSFWorkbook(XSSFWorkbookType.XLSM)) { wb.createSheet("Approved"); macro = serialize(wb); }
    var vba = parts(table());
    vba.put("xl/vbaProject.bin", new byte[]{0});
    for (byte[] bytes : List.of(ole.toByteArray(), encrypted.toByteArray(), macro, zip(vba))) {
      rejection(reader(bytes), bundle(bytes, APPROVED, Map.of()), "ING_MANAGED_XLSX_INVALID", AdapterSpi.ErrorClass.DATA);
    }
  }

  static Map<String,byte[]> parts(byte[] bytes) throws IOException {
    var parts = new LinkedHashMap<String,byte[]>();
    try (var in = new ZipInputStream(new ByteArrayInputStream(bytes))) {
      for (ZipEntry e; (e = in.getNextEntry()) != null;) parts.put(e.getName(), in.readAllBytes());
    }
    return parts;
  }

  static byte[] zip(Map<String,byte[]> parts) throws IOException {
    var out = new ByteArrayOutputStream();
    try (var zip = new ZipOutputStream(out)) {
      for (var part : parts.entrySet()) {
        zip.putNextEntry(new ZipEntry(part.getKey()));
        zip.write(part.getValue());
        zip.closeEntry();
      }
    }
    return out.toByteArray();
  }
}
