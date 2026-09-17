package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.managed.ShapefileReader;
import java.security.MessageDigest;
import java.util.*;
import static it.comune.trieste.ouf.ingestion.AdapterSpi.*;

public final class ManagedShapefileAdapter implements AdapterSpi {
  private final RuntimePorts.ManagedObjectPort objects;
  public ManagedShapefileAdapter(RuntimePorts.ManagedObjectPort objects){this.objects=objects;}
  public String adapterId(){return "managed-shapefile-v1";}
  public Set<String> acquisitionModes(){return Set.of("INTERNAL_MANAGED_SHAPEFILE");}
  public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}
  public RecordCursor open(ExecutionBundle bundle,Checkpoint checkpoint){
    var c=bundle.configuration();String layerName=text(c,"layer"),hash=text(c,"expectedHash");
    long size=integer(c,"expectedSize",1,10*1024*1024);
    if(!(c.get("identityFields") instanceof List<?> raw)||raw.isEmpty()||raw.size()>256||raw.stream().anyMatch(k->!(k instanceof String))||new HashSet<>(raw).size()!=raw.size())throw failure("IDENTITY_INVALID");
    var keys=raw.stream().map(String.class::cast).toList();
    byte[] bytes=objects.read(bundle.bindingRef(),size,hash);
    try{if(bytes.length!=size||!hash.equals("sha256:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))))throw failure("INTEGRITY_MISMATCH");}catch(java.security.NoSuchAlgorithmException e){throw new IllegalStateException(e);}
    var records=new ArrayList<SourceRecord>();var identities=new HashSet<String>();
    try(var reader=new ShapefileReader(bytes)){
      var layer=reader.layers().stream().filter(l->l.name().equals(layerName)).findFirst().orElseThrow(()->failure("LAYER_MISSING"));
      if(!layer.crs().equals(text(c,"sourceCrs"))||!layer.encoding().equals(text(c,"encoding"))||!layer.geometryColumn().equals(text(c,"geometryColumn")))throw failure("PROFILE_MISMATCH");
      if(keys.contains(layer.geometryColumn())||!layer.columns().containsAll(keys))throw failure("IDENTITY_INVALID");
      for(var row:layer.rows()){
        String id=ManagedGeoPackageAdapter.identity(bundle.sourceId(),layerName,row,keys).replace("managed-gpkg:","managed-shp:");
        if(!identities.add(id))throw failure("DUPLICATE_IDENTITY");
        records.add(new SourceRecord(id,records.size()+1,row,Map.of("managedObjectRef",bundle.bindingRef(),"contentHash",hash,"layer",layerName,"sourceCrs",layer.crs(),"encoding",layer.encoding(),"readerVersion","geotools-35.0")));
      }
    }catch(IllegalArgumentException e){throw failure("INVALID_OR_UNSUPPORTED");}
    int offset=checkpoint==null||checkpoint.value().isEmpty()?0:(int)integer(checkpoint.value(),"ordinal",0,records.size());
    return new RecordCursor(){int position=offset;public Optional<SourceRecord> next(){return position<records.size()?Optional.of(records.get(position++)):Optional.empty();}public Checkpoint checkpoint(){return new Checkpoint(Map.of("ordinal",position));}public void close(){records.clear();}};
  }
  private static String text(Map<String,Object> c,String k){if(!(c.get(k) instanceof String s)||s.isBlank())throw failure("CONFIG_INVALID");return s;}
  private static long integer(Map<String,Object> c,String k,long min,long max){if(!(c.get(k) instanceof Number n)||n.doubleValue()!=n.longValue()||n.longValue()<min||n.longValue()>max)throw failure("CONFIG_INVALID");return n.longValue();}
  private static AdapterException failure(String reason){return new AdapterException("ING_SHP_"+reason,ErrorClass.DATA,"Shapefile does not satisfy the approved profile: "+reason);}
}
