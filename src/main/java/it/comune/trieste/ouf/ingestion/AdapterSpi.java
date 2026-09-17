package it.comune.trieste.ouf.ingestion;
import java.time.Duration;
import java.util.*;

public interface AdapterSpi {
  String adapterId();
  Set<String> acquisitionModes();
  Compatibility compatibility();
  RecordCursor open(ExecutionBundle bundle,Checkpoint checkpoint);
  record Compatibility(String current,String oldestReadableBundleVersion){}
  record Checkpoint(Map<String,Object> value){public Checkpoint{value=Map.copyOf(value);}}
  record SourceRecord(String sourceObjectId,long ordinal,Map<String,Object> payload,Map<String,Object> provenance){public SourceRecord{payload=Collections.unmodifiableMap(new LinkedHashMap<>(payload));provenance=Map.copyOf(provenance);}}
  interface RecordCursor extends AutoCloseable {Optional<SourceRecord> next();Checkpoint checkpoint();@Override void close();}

  enum ErrorClass { TRANSIENT_SOURCE, AUTH_ROUTE, CONFIGURATION, DATA, PROGRAMMING }
  final class AdapterException extends RuntimeException {
    private final String code;private final ErrorClass errorClass;private final Duration retryAfter;
    public AdapterException(String code,ErrorClass errorClass,String safeMessage){this(code,errorClass,safeMessage,null);}
    public AdapterException(String code,ErrorClass errorClass,String safeMessage,Duration retryAfter){super(safeMessage);this.code=code;this.errorClass=errorClass;this.retryAfter=retryAfter;}
    public String code(){return code;}public ErrorClass errorClass(){return errorClass;}public Optional<Duration> retryAfter(){return Optional.ofNullable(retryAfter);}
  }
}
