package it.comune.trieste.ouf.ingestion;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Locale;

/** Finite, versioned lexical conversions. No inference, locale defaults or I/O. */
final class ConfiguredValueTransform {
  private ConfiguredValueTransform() {}
  private static final DateTimeFormatter DMY=DateTimeFormatter.ofPattern("dd/MM/uuuu",Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
  static Object apply(String rule,Object value) {
    if("IDENTITY".equals(rule))return value;
    if(rule==null||!java.util.Set.of("DECIMAL_DOT_V1","DECIMAL_COMMA_V1","DATE_ISO_V1","DATE_DMY_V1").contains(rule))
      throw new IllegalArgumentException("ING_TRANSFORM_UNSUPPORTED");
    if(!(value instanceof String text))throw invalid();
    try {
      if(rule.startsWith("DECIMAL_")) {
        String decimal=rule.equals("DECIMAL_COMMA_V1")?text.replace(',','.'):text;
        if(text.length()>256||!text.matches(rule.equals("DECIMAL_COMMA_V1")?"-?(0|[1-9][0-9]*)(,[0-9]+)?":"-?(0|[1-9][0-9]*)(\\.[0-9]+)?"))throw invalid();
        BigDecimal result=new BigDecimal(decimal).stripTrailingZeros();
        if(result.precision()+Math.max(-result.scale(),0)>38||Math.max(result.scale(),0)>18)throw invalid();
        return result;
      }
      boolean iso=rule.equals("DATE_ISO_V1");
      if(!text.matches(iso?"[0-9]{4}-[0-9]{2}-[0-9]{2}":"[0-9]{2}/[0-9]{2}/[0-9]{4}"))throw invalid();
      LocalDate date=LocalDate.parse(text,iso?DateTimeFormatter.ISO_LOCAL_DATE:DMY);
      if(date.getYear()<1||date.getYear()>9999)throw invalid();
      return date.toString();
    } catch(RuntimeException failure) {throw invalid();}
  }
  private static IllegalArgumentException invalid(){return new IllegalArgumentException("ING_DATATYPE_COERCION_FAILED");}
}
