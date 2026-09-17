package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.managed.AccessReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import static it.comune.trieste.ouf.ingestion.AdapterSpi.*;

/** Executes one approved table, with stable keys and no linked database access. */
public final class ManagedAccessAdapter implements AdapterSpi {
  private final RuntimePorts.ManagedObjectPort objects;
  public ManagedAccessAdapter(RuntimePorts.ManagedObjectPort objects) { this.objects=objects; }
  public String adapterId(){return "managed-access-v1";}
  public Set<String> acquisitionModes(){return Set.of("INTERNAL_MANAGED_ACCESS");}
  public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}
  public RecordCursor open(ExecutionBundle bundle,Checkpoint checkpoint){
    var config=bundle.configuration();String table=text(config,"layer"),expected=text(config,"expectedHash");
    long size=integer(config,"expectedSize",1,AccessReader.MAX_BYTES);
    List<String> keys=strings(config,"identityFields"),projection=strings(config,"expectedFieldNames");
    if(!projection.containsAll(keys)||keys.contains("$managedRowOrdinal"))throw failure("IDENTITY_INVALID");
    byte[] bytes=objects.read(bundle.bindingRef(),size,expected);
    if(bytes.length!=size||!expected.equals("sha256:"+hash(bytes)))throw failure("INTEGRITY_MISMATCH");
    long maxRows=integer(config,"maxRows",1,10000),maxCellChars=integer(config,"maxCellChars",1,65536);
    var records=new ArrayList<SourceRecord>();var identities=new HashSet<String>();
    try(var reader=new AccessReader(bytes)){
      for(var row:reader.rows(table,projection)){
        if(records.size()>=maxRows||row.values().stream().anyMatch(v->v instanceof String text&&text.length()>maxCellChars))throw failure("APPROVED_LIMIT_EXCEEDED");
        StringBuilder material=new StringBuilder("ACCESS1");append(material,bundle.sourceId());append(material,table);
        for(String key:keys){Object value=row.get(key);if(value==null||String.valueOf(value).isBlank())throw failure("IDENTITY_MISSING");append(material,key);append(material,String.valueOf(value));}
        String id="managed-access:"+hash(material.toString().getBytes(StandardCharsets.UTF_8));
        if(!identities.add(id))throw failure("DUPLICATE_IDENTITY");
        records.add(new SourceRecord(id,records.size()+1,row,Map.of("managedObjectRef",bundle.bindingRef(),"contentHash",expected,"table",table,"readerVersion","jackcess-5.0.0")));
      }
    }catch(IllegalArgumentException e){throw failure("INVALID_OR_UNSUPPORTED");}
    int offset=checkpoint==null||checkpoint.value().isEmpty()?0:(int)integer(checkpoint.value(),"ordinal",0,records.size());
    return new RecordCursor(){int position=offset;public Optional<SourceRecord> next(){return position<records.size()?Optional.of(records.get(position++)):Optional.empty();}public Checkpoint checkpoint(){return new Checkpoint(Map.of("ordinal",position));}public void close(){records.clear();}};
  }
  private static List<String> strings(Map<String,Object> config,String key){if(!(config.get(key) instanceof List<?> list)||list.isEmpty()||list.size()>256||list.stream().anyMatch(v->!(v instanceof String s)||s.isBlank())||new HashSet<>(list).size()!=list.size())throw failure("CONFIG_INVALID");return list.stream().map(String.class::cast).toList();}
  private static String text(Map<String,Object> config,String key){if(!(config.get(key) instanceof String s)||s.isBlank())throw failure("CONFIG_INVALID");return s;}
  private static long integer(Map<String,Object> config,String key,long min,long max){if(!(config.get(key) instanceof Number n)||n.longValue()<min||n.longValue()>max||n.doubleValue()!=n.longValue())throw failure("CONFIG_INVALID");return n.longValue();}
  private static void append(StringBuilder b,String value){b.append(value.length()).append(':').append(value);}
  private static String hash(byte[] bytes){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));}catch(Exception e){throw new IllegalStateException(e);}}
  private static AdapterException failure(String reason){return new AdapterException("ING_ACCESS_"+reason,ErrorClass.DATA,"Access input does not satisfy the approved profile: "+reason);}
}
