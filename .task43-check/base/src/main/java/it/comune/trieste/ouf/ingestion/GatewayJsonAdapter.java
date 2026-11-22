package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.AdapterSpi.*;
import static it.comune.trieste.ouf.ingestion.ExternalAdapterSupport.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import java.util.*;

public final class GatewayJsonAdapter implements AdapterSpi {
  private final RuntimePorts.GatewaySourcePort gateway;private final ObjectMapper json;
  public GatewayJsonAdapter(RuntimePorts.GatewaySourcePort gateway,ObjectMapper json){this.gateway=Objects.requireNonNull(gateway);this.json=Objects.requireNonNull(json);}
  public String adapterId(){return "gateway-rest-json-v1";}public Set<String> acquisitionModes(){return Set.of("REST_JSON");}public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}
  public RecordCursor open(ExecutionBundle b,Checkpoint checkpoint){requireGateway(b);return new Cursor(b,checkpoint==null?Map.of():checkpoint.value());}
  private final class Cursor implements RecordCursor{
    private final ExecutionBundle b;private final int pageSize,maxPages,maxRecords,maxBytes;private final String itemsPointer,nextPointer,typeCode,correlation;private final List<String> keys;private final Deque<SourceRecord> buffer=new ArrayDeque<>();private String pageToken,nextToken;private int pages,records,index,pageOffset;private boolean loaded,done;
    Cursor(ExecutionBundle b,Map<String,Object> checkpoint){this.b=b;Map<String,Object> c=b.configuration();pageSize=integer(c,"pageSize",100,1,10_000);maxPages=integer(c,"maxPages",1000,1,100_000);maxRecords=integer(c,"maxRecords",100_000,1,1_000_000);maxBytes=integer(c,"maxPageBytes",5_000_000,1,50_000_000);itemsPointer=text(c,"itemsPointer","/items");nextPointer=text(c,"nextTokenPointer","/nextPageToken");typeCode=text(c,"typeCode","OBJECT");correlation=text(c,"correlationId","runtime");keys=strings(c,"identityFields");pageToken=checkpoint.get("pageToken") instanceof String s?s:null;index=checkpoint.get("recordIndex") instanceof Number n?n.intValue():0;records=index;pageOffset=checkpoint.get("pageOffset") instanceof Number n?n.intValue():0;}
    public Optional<SourceRecord> next(){while(buffer.isEmpty()&&!done){if(loaded){pageToken=nextToken;pageOffset=0;loaded=false;if(pageToken==null){done=true;break;}}fetch();}if(buffer.isEmpty())return Optional.empty();records++;pageOffset++;return Optional.of(buffer.removeFirst());}
    private void fetch(){if(++pages>maxPages)throw error("ING_PAGE_LIMIT_EXCEEDED",ErrorClass.DATA);Map<String,Object> request=new LinkedHashMap<>();request.put("pageSize",pageSize);if(pageToken!=null)request.put("pageToken",pageToken);byte[] bytes;try{bytes=gateway.fetch(b.bindingRef(),request,correlation);}catch(RuntimePorts.GatewayFailure e){throw new AdapterException(e.safeCode(),e.errorClass(),e.safeCode(),e.retryAfter().orElse(null));}if(bytes.length>maxBytes)throw error("ING_PAGE_TOO_LARGE",ErrorClass.DATA);try{JsonNode root=json.readTree(bytes),items=root.at(itemsPointer);if(!items.isArray()||pageOffset>items.size())throw error("ING_JSON_ITEMS_INVALID",ErrorClass.DATA);int position=0;for(JsonNode item:items){if(position++<pageOffset)continue;if(!item.isObject())throw error("ING_JSON_RECORD_INVALID",ErrorClass.DATA);Map<String,Object> row=json.convertValue(item,new TypeReference<>(){});if(records+buffer.size()>=maxRecords)throw error("ING_RECORD_LIMIT_EXCEEDED",ErrorClass.DATA);String id=identity(b.sourceId(),typeCode,row,keys);buffer.add(new SourceRecord(id,index++,row,Map.of("bindingRef",b.bindingRef(),"page",pages)));}JsonNode next=root.at(nextPointer);nextToken=next.isTextual()&&!next.textValue().isBlank()?next.textValue():null;loaded=true;}catch(AdapterException e){throw e;}catch(Exception e){throw error("ING_JSON_INVALID",ErrorClass.DATA);}}
    public Checkpoint checkpoint(){Map<String,Object> cp=new LinkedHashMap<>();cp.put("recordIndex",records);cp.put("pageOffset",pageOffset);if(pageToken!=null)cp.put("pageToken",pageToken);return new Checkpoint(cp);}public void close(){buffer.clear();done=true;}
  }
}
