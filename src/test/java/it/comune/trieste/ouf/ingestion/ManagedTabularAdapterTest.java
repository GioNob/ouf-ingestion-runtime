package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;

class ManagedTabularAdapterTest {
  @Test void csvMakesExactlyOneObjectPerRowAndResumesAfterCheckpoint() throws Exception {
    byte[] bytes="id,name\nA,Alpha\nB,Beta\n".getBytes(StandardCharsets.UTF_8);
    ManagedTabularAdapter adapter=adapter(bytes);
    ExecutionBundle b=bundle("INTERNAL_MANAGED_CSV",bytes,Map.of("identityFields",List.of("id")));
    try(AdapterSpi.RecordCursor c=adapter.open(b,new AdapterSpi.Checkpoint(Map.of("ordinal",1)))){
      AdapterSpi.SourceRecord row=c.next().orElseThrow();
      assertThat(row.payload()).containsEntry("id","B").containsEntry("name","Beta");
      assertThat(c.next()).isEmpty();assertThat(c.checkpoint().value()).containsEntry("ordinal",2);
    }
  }

  @Test void keyedIdentitySurvivesRowReordering() throws Exception {
    byte[] one="id,name\nA,Alpha\nB,Beta\n".getBytes(StandardCharsets.UTF_8);
    byte[] two="id,name\nB,Beta\nA,Alpha\n".getBytes(StandardCharsets.UTF_8);
    assertThat(ids(one)).containsExactlyInAnyOrderElementsOf(ids(two));
  }

  @Test void duplicateConfiguredIdentityIsRejected() throws Exception {
    byte[] bytes="id,name\nA,Alpha\nA,Again\n".getBytes(StandardCharsets.UTF_8);
    assertThatThrownBy(()->adapter(bytes).open(bundle("INTERNAL_MANAGED_CSV",bytes,Map.of("identityFields",List.of("id"))),new AdapterSpi.Checkpoint(Map.of("ordinal",0))))
      .isInstanceOf(AdapterSpi.AdapterException.class).hasMessage("ING_MANAGED_DUPLICATE_IDENTITY");
  }

  @Test void xlsxUsesExplicitSheetAndNeverEvaluatesFormula() throws Exception {
    byte[] bytes;
    try(var wb=new XSSFWorkbook();var out=new ByteArrayOutputStream()){
      var s=wb.createSheet("objects");s.createRow(0).createCell(0).setCellValue("id");s.createRow(1).createCell(0).setCellFormula("1+1");wb.write(out);bytes=out.toByteArray();
    }
    ExecutionBundle b=bundle("INTERNAL_MANAGED_XLSX",bytes,Map.of("sheet","objects","identityFields",List.of("id")));
    assertThatThrownBy(()->adapter(bytes).open(b,new AdapterSpi.Checkpoint(Map.of("ordinal",0))))
      .isInstanceOf(AdapterSpi.AdapterException.class).hasMessage("ING_MANAGED_FORMULA_FORBIDDEN");
  }

  private List<String> ids(byte[] bytes){
    var c=adapter(bytes).open(bundle("INTERNAL_MANAGED_CSV",bytes,Map.of("identityFields",List.of("id"))),new AdapterSpi.Checkpoint(Map.of("ordinal",0)));
    List<String> ids=new ArrayList<>();for(var r=c.next();r.isPresent();r=c.next())ids.add(r.get().sourceObjectId());return ids;
  }
  private ManagedTabularAdapter adapter(byte[] bytes){return new ManagedTabularAdapter((ref,size,hash)->bytes);}
  private ExecutionBundle bundle(String mode,byte[] bytes,Map<String,Object> extra){
    Map<String,Object> c=new HashMap<>(extra);c.put("expectedSize",(long)bytes.length);c.put("expectedHash",sha(bytes));c.put("maxRows",100);c.put("maxColumns",20);c.put("maxCellChars",1000);
    return new ExecutionBundle("bundle","1",sha(bytes),"source-1",mode,"object:1",c);
  }
  private static String sha(byte[] bytes){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));}catch(Exception e){throw new IllegalStateException(e);}}
}
