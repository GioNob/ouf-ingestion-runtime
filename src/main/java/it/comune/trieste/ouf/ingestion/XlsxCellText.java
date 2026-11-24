package it.comune.trieste.ouf.ingestion;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;

/** Lexical values shared with the Onboarding reader through the v1 contract vectors. */
final class XlsxCellText {
  static final String RULE = "ouf.xlsx-cell-text.v1";
  private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);
  private static final DateTimeFormatter DATETIME =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssXXX", Locale.ROOT);
  private static final Pattern TOKENS = Pattern.compile(
      "\"[^\"]*\"|\\\\.|\\[[^\\]]*\\]|AM/PM|y+|d+|m+|h+|s+", Pattern.CASE_INSENSITIVE);
  private enum Format { NUMBER, DATE, TIME, DATETIME }

  private XlsxCellText() {}

  static String read(XSSFCell cell, boolean date1904, ZoneId zone) {
    if (cell == null) return "";
    CellType type = cell.getCellType();
    boolean formula = type == CellType.FORMULA;
    if (formula) {
      if (!cell.getCTCell().isSetV()) throw data("FORMULA_WITHOUT_CACHED_VALUE");
      type = cell.getCachedFormulaResultType();
    }
    return switch (type) {
      case BLANK -> "";
      case STRING -> formula && (cell.getRawValue() == null || cell.getRawValue().isEmpty())
          ? "" : cell.getStringCellValue();
      case ERROR -> throw data("CELL_ERROR");
      case BOOLEAN -> {
        if (formula && !("0".equals(cell.getRawValue()) || "1".equals(cell.getRawValue()))) {
          throw data("FORMULA_WITHOUT_CACHED_VALUE");
        }
        yield Boolean.toString(cell.getBooleanCellValue());
      }
      case NUMERIC -> {
        double value;
        try {
          // POI's numeric getter maps an empty cached <v/> to zero.
          if (formula && (cell.getRawValue() == null || cell.getRawValue().isEmpty())) throw data("FORMULA_WITHOUT_CACHED_VALUE");
          value = formula ? new BigDecimal(cell.getRawValue()).doubleValue() : cell.getNumericCellValue();
          if (!Double.isFinite(value)) throw new NumberFormatException();
        } catch (NumberFormatException e) {
          throw data(formula ? "FORMULA_WITHOUT_CACHED_VALUE" : "CELL_ERROR");
        }
        yield number(value, classify(cell.getCellStyle().getDataFormatString()), date1904, zone);
      }
      default -> throw data("CELL_ERROR");
    };
  }

  private static Format classify(String pattern) {
    if (pattern == null) return Format.NUMBER;
    var tokens = TOKENS.matcher(pattern);
    StringBuilder fields = new StringBuilder();
    while (tokens.find()) {
      String token = tokens.group().toLowerCase(Locale.ROOT);
      switch (token.charAt(0)) {
        case '[' -> {
          if (token.matches("\\[(h+|m+|s+)\\]")) return Format.NUMBER;
        }
        case '"', '\\' -> { /* display literal */ }
        default -> fields.append(token.charAt(0));
      }
    }
    boolean date = false, time = false;
    for (int i = 0; i < fields.length(); i++) {
      char field = fields.charAt(i);
      if (field == 'm') {
        boolean minute = (i > 0 && fields.charAt(i - 1) == 'h')
            || (i + 1 < fields.length() && fields.charAt(i + 1) == 's');
        if (minute) time = true;
        else date = true;
      } else if (field == 'y' || field == 'd') date = true;
      else time = true;
    }
    if (date) return time ? Format.DATETIME : Format.DATE;
    return time ? Format.TIME : Format.NUMBER;
  }

  private static String number(double value, Format format, boolean date1904, ZoneId zone) {
    if (format == Format.NUMBER) return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    // Upper bounds are the first day of year 10000 in the respective Excel systems.
    if (value < 0 || value >= (date1904 ? 2957004 : 2958466)) throw data("DATE_OUT_OF_RANGE");
    if (format == Format.TIME) {
      return TIME.format(DateUtil.getLocalDateTime(value - Math.floor(value), date1904, true));
    }
    if (!date1904 && value < 61) throw data("DATE_OUT_OF_RANGE");
    LocalDateTime local = DateUtil.getLocalDateTime(value, date1904, true);
    if (local.getYear() > 9999) throw data("DATE_OUT_OF_RANGE");
    if (format == Format.DATE) return local.toLocalDate().toString();
    if (zone == null) throw data("TIME_ZONE_UNDECLARED");
    return DATETIME.format(ZonedDateTime.ofLocal(local, zone, null));
  }

  static AdapterSpi.AdapterException data(String suffix) {
    String code = "ING_MANAGED_XLSX_" + suffix;
    return new AdapterSpi.AdapterException(code, AdapterSpi.ErrorClass.DATA, code);
  }
}
