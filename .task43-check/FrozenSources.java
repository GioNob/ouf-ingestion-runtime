package it.comune.trieste.ouf.ingestion;
import java.util.*;import java.security.*;import java.nio.charset.*;
class ExecutionGatewayClient {static String hash(byte[] bytes){try{return "sha256:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));}catch(Exception e){throw new AssertionError();}}}
class ManagedTabularAdapter implements AdapterSpi {
 final RuntimePorts.ManagedObjectPort objects;ManagedTabularAdapter(RuntimePorts.ManagedObjectPort objects){this.objects=objects;}
 public String adapterId(){return "managed-tabular-v1";} public Set<String> acquisitionModes(){return Set.of("INTERNAL_MANAGED_CSV");}
 public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}
 public RecordCursor open(ExecutionBundle b,Checkpoint cp){
  String[] rows=new String(objects.read(b.bindingRef(),(int)b.configuration().get("expectedSize"),(String)b.configuration().get("expectedHash")), StandardCharsets.UTF_8).split("\n");
  return new RecordCursor(){int i=0;public Optional<SourceRecord> next(){if(++i==rows.length)return Optional.empty();return Optional.of(new SourceRecord("obj"+i,i,Map.of("x",rows[i]),Map.of()));} public Checkpoint checkpoint(){return cp;}public void close(){}};
 }
}
class ManagedGeoPackageAdapter extends ManagedTabularAdapter {ManagedGeoPackageAdapter(RuntimePorts.ManagedObjectPort p){super(p);}}
