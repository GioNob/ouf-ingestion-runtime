package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.math.BigDecimal;
import java.util.*;
import org.junit.jupiter.api.Test;

class ConfiguredValueTransformTest {
  @Test void identityPreservesHistoricalValuesAndNull(){
    Object value=Map.of("nested",List.of("unmodified"));
    assertThat(ConfiguredValueTransform.apply("IDENTITY",value)).isSameAs(value);
    assertThat(ConfiguredValueTransform.apply("IDENTITY",null)).isNull();
  }
  @Test void declaredDecimalSeparatorIsRequiredAndCanonicalValueIsNumeric(){
    assertThat(ConfiguredValueTransform.apply("DECIMAL_COMMA_V1","-12,3400")).isEqualTo(new BigDecimal("-12.34"));
    assertThat(ConfiguredValueTransform.apply("DECIMAL_DOT_V1","12.3400")).isEqualTo(new BigDecimal("12.34"));
    for(String value:List.of("12,34"," 12.34","12.34 ","1e3","1,234.56","01.2","NaN","","+1.0"))
      assertThatThrownBy(()->ConfiguredValueTransform.apply("DECIMAL_DOT_V1",value)).hasMessage("ING_DATATYPE_COERCION_FAILED");
  }
  @Test void explicitDateFormatsAreStrictAndNeverInventTimezoneOrValidity(){
    assertThat(ConfiguredValueTransform.apply("DATE_DMY_V1","29/02/2024")).isEqualTo("2024-02-29");
    assertThat(ConfiguredValueTransform.apply("DATE_ISO_V1","2024-02-29")).isEqualTo("2024-02-29");
    for(String value:List.of("29/02/2023","31/04/2024","1/02/2024","2024-02-29","29/02/0000"))
      assertThatThrownBy(()->ConfiguredValueTransform.apply("DATE_DMY_V1",value)).hasMessage("ING_DATATYPE_COERCION_FAILED");
  }
  @Test void unknownRulesNonLexicalInputsAndNumericOverflowBlock(){
    assertThatThrownBy(()->ConfiguredValueTransform.apply("GUESS", "1")).hasMessage("ING_TRANSFORM_UNSUPPORTED");
    for(Object value:Arrays.asList(null,1,true,Map.of()))
      assertThatThrownBy(()->ConfiguredValueTransform.apply("DECIMAL_DOT_V1",value)).hasMessage("ING_DATATYPE_COERCION_FAILED");
    for(String value:List.of("1".repeat(39),"1"+"0".repeat(38),"0.0000000000000000001","1".repeat(257)))
      assertThatThrownBy(()->ConfiguredValueTransform.apply("DECIMAL_DOT_V1",value)).hasMessage("ING_DATATYPE_COERCION_FAILED");
  }
}
