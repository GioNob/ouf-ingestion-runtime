package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.XlsxCellText.data;

import java.io.*;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.utils.InputStreamStatistics;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.util.ZipEntrySource;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.*;

/** Reads exactly the worksheet and lexical profile pinned in the published bundle. */
public final class ManagedXlsxAdapter implements AdapterSpi {
  static final String ID = "managed-xlsx-v1";
  static final String MODE = "INTERNAL_MANAGED_XLSX";
  private final RuntimePorts.ManagedObjectPort objects;
  private final ArchiveLimits archive;

  public ManagedXlsxAdapter(RuntimePorts.ManagedObjectPort objects) {
    this(objects, ArchiveLimits.installed());
  }

  ManagedXlsxAdapter(RuntimePorts.ManagedObjectPort objects, ArchiveLimits archive) {
    this.objects = Objects.requireNonNull(objects);
    this.archive = archive;
  }

  public String adapterId() { return ID; }
  public Set<String> acquisitionModes() { return Set.of(MODE); }
  public Compatibility compatibility() { return new Compatibility("1.0.0", "1.0.0"); }

  record Parameters(String sheet, int sheetIndex, int headerRow, boolean date1904, ZoneId zone) {
    static Parameters parse(Object input) {
      if (!(input instanceof Map<?,?> root) || root.size() != 1
          || !(root.get("xlsx") instanceof Map<?,?> xlsx)
          || !Set.of("sheet", "sheetIndex", "headerRow", "dateSystem", "cellText", "timeZone")
              .containsAll(xlsx.keySet())
          || !(xlsx.get("sheet") instanceof String name) || name.isEmpty() || name.length() > 31
          || !XlsxCellText.RULE.equals(xlsx.get("cellText"))
          || !("1900".equals(xlsx.get("dateSystem")) || "1904".equals(xlsx.get("dateSystem")))) {
        throw mismatch();
      }
      int index = integer(xlsx.get("sheetIndex"), 0, Integer.MAX_VALUE, "ING_MANAGED_XLSX_PARAMETERS_MISMATCH");
      int header = integer(xlsx.get("headerRow"), 1, 1_048_576, "ING_MANAGED_XLSX_PARAMETERS_MISMATCH");
      ZoneId zone = null;
      if (xlsx.containsKey("timeZone")) {
        if (!(xlsx.get("timeZone") instanceof String id) || !ZoneId.getAvailableZoneIds().contains(id)) throw mismatch();
        zone = ZoneId.of(id);
      }
      return new Parameters(name, index, header, "1904".equals(xlsx.get("dateSystem")), zone);
    }
  }

  record ArchiveLimits(int fileBytes, int totalBytes, int entryBytes, double minRatio, int entries, int sheets) {
    private static final int MIB = 1 << 20;

    ArchiveLimits {
      if (fileBytes < 1 || fileBytes > 10 * MIB || totalBytes < 1 || totalBytes > 256 * MIB
          || entryBytes < 1 || entryBytes > 256 * MIB || !Double.isFinite(minRatio) || minRatio <= 0
          || entries < 1 || entries > 10_000 || sheets < 1 || sheets > 64) {
        throw config("ING_MANAGED_XLSX_LIMIT_CONFIG_INVALID");
      }
    }

    static ArchiveLimits installed() {
      // The worker, Spring readiness probe and standalone probe use the same installation profile.
      var defaults = ResourceBundle.getBundle("managed-xlsx", Locale.ROOT);
      Function<String,String> property = name -> {
        String key = "ouf.ingestion.xlsx." + name;
        String env = System.getenv("OUF_ING_XLSX_" + name.toUpperCase(Locale.ROOT).replace('-', '_'));
        return System.getProperty(key, env == null ? defaults.getString(key) : env);
      };
      try {
        return new ArchiveLimits(Integer.parseInt(property.apply("max-file-bytes")),
            Integer.parseInt(property.apply("max-total-bytes")), Integer.parseInt(property.apply("max-entry-bytes")),
            Double.parseDouble(property.apply("min-inflate-ratio")), Integer.parseInt(property.apply("max-entries")),
            Integer.parseInt(property.apply("max-sheets")));
      } catch (NumberFormatException e) {
        throw config("ING_MANAGED_XLSX_LIMIT_CONFIG_INVALID");
      }
    }

  }

  private record RowLimits(int rows, int columns, int chars) {}

  public RecordCursor open(ExecutionBundle bundle, Checkpoint checkpoint) {
    if (!MODE.equals(bundle.acquisitionMode())) throw config("ING_MANAGED_MODE_UNSUPPORTED");
    var c = bundle.configuration();
    Parameters p = Parameters.parse(c.get("readParameters"));
    int size = integer(c.get("expectedSize"), 1, Integer.MAX_VALUE, "ING_MANAGED_CONFIG_INVALID");
    if (size > archive.fileBytes) throw data("FILE_LIMIT");
    if (!(c.get("expectedHash") instanceof String hash) || hash.isBlank()) throw config("ING_MANAGED_CONFIG_MISSING");
    var limits = new RowLimits(limit(c, "maxRows", 1_000_000), limit(c, "maxColumns", 10_000),
        limit(c, "maxCellChars", 1_000_000));
    if (!(c.getOrDefault("identityFields", List.of()) instanceof List<?> fields)
        || fields.stream().anyMatch(f -> !(f instanceof String))) throw config("ING_MANAGED_CONFIG_INVALID");
    List<String> keys = fields.stream().map(String.class::cast).toList();
    int resume = checkpoint == null || checkpoint.value().isEmpty() ? 0
        : integer(checkpoint.value().get("ordinal"), 0, Integer.MAX_VALUE, "ING_CHECKPOINT_OUT_OF_RANGE");
    byte[] bytes = objects.read(bundle.bindingRef(), size, hash);
    if (bytes.length != size) throw new AdapterException("ING_MANAGED_SIZE_MISMATCH", ErrorClass.DATA, "ING_MANAGED_SIZE_MISMATCH");

    try (CheckedZip zip = new CheckedZip(bytes)) {
      zip.validate(archive);
      try (OPCPackage pkg = OPCPackage.open(zip)) {
        var workbookPart = pkg.getPart(org.apache.poi.openxml4j.opc.PackagingURIHelper.createPartName("/xl/workbook.xml"));
        if (workbookPart == null || !XSSFRelation.WORKBOOK.getContentType().equals(workbookPart.getContentType())
            || !pkg.getPartsByContentType("application/vnd.ms-office.vbaProject").isEmpty()) throw data("INVALID");
        try (var workbook = new XSSFWorkbook(pkg)) {
          if (!workbook.getPackagePart().equals(workbookPart)) throw data("INVALID");
          int worksheets = 0;
          for (var item : workbook) if (!(item instanceof XSSFChartSheet)) worksheets++;
          if (worksheets > archive.sheets) throw data("SHEET_LIMIT");
          XSSFSheet sheet = workbook.getSheet(p.sheet);
          if (sheet == null || sheet instanceof XSSFChartSheet || !sheet.getSheetName().equals(p.sheet)
              || workbook.getSheetIndex(sheet) != p.sheetIndex || workbook.isDate1904() != p.date1904) throw mismatch();
          return records(bundle, sheet, p, limits, hash, keys, resume);
        }
      }
    } catch (AdapterException e) {
      throw e;
    } catch (Exception e) {
      throw data("INVALID");
    }
  }

  /** One archive view for validation and POI, without changing process-wide ZipSecureFile settings. */
  private static final class CheckedZip implements ZipEntrySource {
    private final ZipFile zip;
    private final Map<String,ZipArchiveEntry> entriesByName = new HashMap<>();
    private boolean closed;

    CheckedZip(byte[] bytes) throws IOException {
      zip = ZipFile.builder().setSeekableByteChannel(new SeekableInMemoryByteChannel(bytes)).get();
    }
    public Enumeration<ZipArchiveEntry> getEntries() { return zip.getEntries(); }
    public ZipArchiveEntry getEntry(String name) {
      String path = name.replace('\\', '/');
      if (path.startsWith("/")) path = path.substring(1);
      return entriesByName.get(path.toLowerCase(Locale.ROOT));
    }
    public InputStream getInputStream(ZipArchiveEntry entry) throws IOException { return zip.getInputStream(entry); }
    public boolean isClosed() { return closed; }
    public void close() throws IOException {
      if (!closed) { closed = true; zip.close(); }
    }

    void validate(ArchiveLimits limits) throws IOException {
      byte[] buffer = new byte[8192];
      long total = 0;
      var entries = getEntries();
      while (entries.hasMoreElements()) {
        ZipArchiveEntry entry = entries.nextElement();
        String name = entry.getName().toLowerCase(Locale.ROOT);
        if (entriesByName.putIfAbsent(name, entry) != null || name.equals("vbaproject.bin") || name.endsWith("/vbaproject.bin")
            || entry.getGeneralPurposeBit().usesEncryption()
            || (entry.getMethod() != ZipEntry.DEFLATED && entry.getMethod() != ZipEntry.STORED)) throw data("INVALID");
        if (entriesByName.size() > limits.entries) throw data("ENTRY_COUNT_LIMIT");
        CRC32 crc = new CRC32();
        try (var in = getInputStream(entry)) {
          long length = 0;
          for (int n; (n = in.read(buffer)) != -1;) {
            length += n;
            total += n;
            if (length > limits.entryBytes || total > limits.totalBytes) throw data("INFLATED_SIZE_LIMIT");
            crc.update(buffer, 0, n);
          }
          long compressed = in instanceof InputStreamStatistics stats ? stats.getCompressedCount() : entry.getCompressedSize();
          if (compressed < length * limits.minRatio) throw data("INFLATE_RATIO_LIMIT");
          if (length != entry.getSize() || crc.getValue() != entry.getCrc()) throw data("INVALID");
        }
      }
      if (zip.getEntry("[Content_Types].xml") == null || zip.getEntry("xl/workbook.xml") == null) throw data("INVALID");
    }
  }

  private static RecordCursor records(ExecutionBundle bundle, XSSFSheet sheet, Parameters p, RowLimits limits,
                                      String hash, List<String> keys, int resume) {
    int headerIndex = p.headerRow - 1;
    for (var region : sheet.getMergedRegions()) {
      if (region.getLastRow() >= headerIndex) {
        throw new AdapterException("ING_MANAGED_XLSX_MERGED_CELLS", ErrorClass.DATA,
            "ING_MANAGED_XLSX_MERGED_CELLS " + region.formatAsString());
      }
    }
    List<String> headers = headers(sheet, p, limits);
    List<SourceRecord> rows = new ArrayList<>();
    Set<String> identities = new HashSet<>();
    int position = 0;
    for (Row row : sheet) {
      int ordinal = row.getRowNum();
      if (ordinal <= headerIndex) continue;
      // Store only populated cells during admission. Wide, sparse sheets expand one row at a time at emission.
      var values = new LinkedHashMap<String,Object>();
      for (Cell cell : row) {
        String value = checked((XSSFCell) cell, p, limits);
        if (value.isEmpty()) continue;
        if (cell.getColumnIndex() >= headers.size()) throw data("DATA_OUTSIDE_HEADER");
        values.put(headers.get(cell.getColumnIndex()), value);
      }
      if (values.isEmpty()) continue;
      if (rows.size() >= limits.rows) throw data("ROW_LIMIT");
      String id = ManagedIdentity.sourceObjectId(bundle.sourceId(), hash, p.sheet, ordinal, values, keys);
      if (!identities.add(id)) throw new AdapterException("ING_MANAGED_DUPLICATE_IDENTITY", ErrorClass.DATA,
          "ING_MANAGED_DUPLICATE_IDENTITY");
      rows.add(new SourceRecord(id, ordinal, values, Map.of("managedObjectRef", bundle.bindingRef(),
          "sheet", p.sheet, "rowOrdinal", (long) ordinal)));
      if (ordinal == resume) position = rows.size();
    }
    if (resume != 0 && position == 0) throw config("ING_CHECKPOINT_OUT_OF_RANGE");
    return new Cursor(headers, rows, position);
  }

  private static List<String> headers(XSSFSheet sheet, Parameters p, RowLimits limits) {
    XSSFRow header = sheet.getRow(p.headerRow - 1);
    var names = new ArrayList<String>();
    var unique = new HashSet<String>();
    if (header != null) {
      for (Cell cell : header) {
        String value = checked((XSSFCell) cell, p, limits);
        if (value.isEmpty()) continue;
        int column = cell.getColumnIndex();
        if (column >= limits.columns) throw data("COLUMN_LIMIT");
        String name = value.strip();
        if (column != names.size() || name.isEmpty() || name.length() > 256 || !unique.add(name)) {
          throw data("HEADER_INVALID");
        }
        names.add(name);
      }
    }
    if (names.isEmpty()) {
      for (Row row : sheet) {
        if (row.getRowNum() < p.headerRow) continue;
        for (Cell cell : row) {
          if (!checked((XSSFCell) cell, p, limits).isEmpty()) throw data("HEADER_INVALID");
        }
      }
      throw data("EMPTY_SHEET");
    }
    return names;
  }

  private static String checked(XSSFCell cell, Parameters p, RowLimits limits) {
    String value = XlsxCellText.read(cell, p.date1904, p.zone);
    if (value.length() > limits.chars) throw new AdapterException("ING_MANAGED_CELL_TOO_LARGE", ErrorClass.DATA,
        "ING_MANAGED_CELL_TOO_LARGE");
    return value;
  }

  private static final class Cursor implements RecordCursor {
    private final List<String> headers;
    private final List<SourceRecord> rows;
    private int position;
    Cursor(List<String> headers, List<SourceRecord> rows, int position) {
      this.headers = headers;
      this.rows = rows;
      this.position = position;
    }
    public Optional<SourceRecord> next() {
      if (position == rows.size()) return Optional.empty();
      var sparse = rows.get(position++);
      Map<String,Object> full = new LinkedHashMap<>();
      for (String name : headers) full.put(name, sparse.payload().get(name));
      return Optional.of(new SourceRecord(sparse.sourceObjectId(), sparse.ordinal(), full, sparse.provenance()));
    }
    public Checkpoint checkpoint() {
      return new Checkpoint(Map.of("ordinal", position == 0 ? 0 : rows.get(position - 1).ordinal()));
    }
    public void close() {}
  }

  private static int limit(Map<String,Object> config, String key, int max) {
    return integer(config.get(key), 1, max, "ING_MANAGED_CONFIG_INVALID");
  }
  private static int integer(Object value, int min, int max, String code) {
    if (!(value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte)) throw config(code);
    long n = ((Number) value).longValue();
    if (n < min || n > max) throw config(code);
    return (int) n;
  }
  private static AdapterException mismatch() { return config("ING_MANAGED_XLSX_PARAMETERS_MISMATCH"); }
  private static AdapterException config(String code) { return new AdapterException(code, ErrorClass.CONFIGURATION, code); }
}
