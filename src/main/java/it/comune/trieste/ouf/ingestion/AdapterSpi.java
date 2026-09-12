package it.comune.trieste.ouf.ingestion;
import java.util.*;

public interface AdapterSpi {
  String adapterId();
  Set<String> acquisitionModes();
  Compatibility compatibility();
  RecordCursor open(ExecutionBundle bundle,Checkpoint checkpoint);
  record Compatibility(String current,String oldestReadableBundleVersion){}
  record Checkpoint(Map<String,Object> value){public Checkpoint{value=Map.copyOf(value);}}
  record SourceRecord(String sourceObjectId,long ordinal,Map<String,Object> payload,Map<String,Object> provenance){public SourceRecord{payload=Map.copyOf(payload);provenance=Map.copyOf(provenance);}}
  interface RecordCursor extends AutoCloseable {Optional<SourceRecord> next();Checkpoint checkpoint();@Override void close();}
}
