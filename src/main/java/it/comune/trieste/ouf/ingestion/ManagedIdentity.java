package it.comune.trieste.ouf.ingestion;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

final class ManagedIdentity {
  private ManagedIdentity() {}

  static String sourceObjectId(String sourceId,String objectHash,String sheet,long ordinal,
                               Map<String,Object> row,List<String> identityFields) {
    String material;
    if(identityFields.isEmpty()) {
      material="ROW\u001f"+objectHash+'\u001f'+sheet+'\u001f'+ordinal;
    } else {
      StringBuilder b=new StringBuilder("KEY");
      for(String field:identityFields) {
        Object value=row.get(field);
        if(value==null||String.valueOf(value).isBlank()) throw new AdapterSpi.AdapterException(
          "ING_MANAGED_IDENTITY_MISSING",AdapterSpi.ErrorClass.DATA,"Configured identity field is missing");
        b.append('\u001f').append(field).append('=').append(normalize(value));
      }
      material=b.toString();
    }
    return "managed:"+sha256(sourceId+'\u001f'+material);
  }

  private static String normalize(Object value){return String.valueOf(value).strip();}
  private static String sha256(String value){
    try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));}
    catch(Exception e){throw new IllegalStateException(e);}
  }
}
