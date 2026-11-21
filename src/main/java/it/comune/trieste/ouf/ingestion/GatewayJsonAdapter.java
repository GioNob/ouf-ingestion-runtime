package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.AdapterSpi.*;
import static it.comune.trieste.ouf.ingestion.ExternalAdapterSupport.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import java.util.*;

public final class GatewayJsonAdapter implements AdapterSpi {
  private final RuntimePorts.GatewaySourcePort gateway;
  private final ObjectMapper json;
  public GatewayJsonAdapter(RuntimePorts.GatewaySourcePort gateway,ObjectMapper json) {
    this.gateway=Objects.requireNonNull(gateway);
    this.json=Objects.requireNonNull(json);
  }
  public String adapterId(){return "gateway-rest-json-v1";}
  public Set<String> acquisitionModes(){return Set.of("REST_JSON");}
  public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}
  public RecordCursor open(ExecutionBundle bundle,Checkpoint checkpoint){return open(bundle,checkpoint,false,null);}

  interface PageGate { void check(PullSchema.Comparison schema, Map<String,Object> position); }

  RecordCursor open(ExecutionBundle bundle, Checkpoint checkpoint, boolean openIncident, PageGate gate) {
    requireGateway(bundle);
    return new Cursor(bundle,checkpoint==null ? Map.of() : checkpoint.value(),openIncident,gate);
  }

  private final class Cursor implements RecordCursor {
    private final ExecutionBundle bundle;
    private final PageGate gate;
    private final boolean openIncident;
    private final Deque<SourceRecord> legacyRows=new ArrayDeque<>();
    private final int pageSize,maxPages,maxRecords,maxBytes;
    private final String itemsPointer,nextPointer,typeCode,correlation,collection;
    private final List<String> keys;
    private final Map<String,Object> initialPosition;
    private String token,next,mode;
    private int pages,offset,read;
    private long ordinal;
    private JsonNode page,items;
    private boolean vertical,done,more;

    Cursor(ExecutionBundle bundle,Map<String,Object> checkpoint,boolean openIncident,PageGate gate) {
      this.bundle=bundle;
      this.gate=gate;
      this.openIncident=openIncident;
      this.initialPosition=checkpoint;
      var config=bundle.configuration();
      JsonNode profile=PullSchema.profile(bundle);
      JsonNode api=profile.path("readParameters").path("api");
      vertical=PullSchema.declaresVerticalContract(bundle,api) || checkpoint.containsKey("mode");
      collection=profile.path("origin").path("collection").asText(config.get("collection") instanceof String name ? name : null);
      pageSize=integer(config,"pageSize",vertical ? api.path("pageSize").asInt(100) : 100,1,10_000);
      maxPages=integer(config,"maxPages",1000,1,100_000);
      maxRecords=integer(config,"maxRecords",100_000,1,1_000_000);
      maxBytes=integer(config,"maxPageBytes",5_000_000,1,50_000_000);
      itemsPointer=text(config,"itemsPointer","/items");
      nextPointer=text(config,"nextTokenPointer","/nextPageToken");
      typeCode=text(config,"typeCode","OBJECT");
      correlation=text(config,"correlationId","runtime");
      keys=strings(config,"identityFields");
      token=checkpoint.get("pageToken") instanceof String savedToken ? savedToken : null;
      offset=checkpoint.get("pageOffset") instanceof Number savedOffset ? savedOffset.intValue() : 0;
      ordinal=checkpoint.get("recordIndex") instanceof Number savedIndex ? savedIndex.longValue() : 0;
      mode=(String)checkpoint.getOrDefault("mode",api.path("mode").asText("CURSOR"));
      if(!vertical) read=Math.toIntExact(ordinal);
      // Never forward an opaque position to a different governed binding or selected collection.
      if(checkpoint.containsKey("bindingRef") && !bundle.bindingRef().equals(checkpoint.get("bindingRef")) ||
          collection!=null && checkpoint.containsKey("collection") && !collection.equals(checkpoint.get("collection"))) {
        token=null;
        offset=0;
      }
    }

    public Optional<SourceRecord> next() {
      while (!done && (items==null || offset==items.size())) {
        if (items!=null) {
          if (!more) {done=true; break;}
          token=next;
          offset=0;
        }
        fetchPage();
      }
      if (done) return Optional.empty();
      SourceRecord record=vertical ? verticalRecord(items.get(offset)) : legacyRows.removeFirst();
      offset++;
      ordinal++;
      read++;
      return Optional.of(record);
    }

    private void fetchPage() {
      if (++pages>maxPages) throw error("ING_PAGE_LIMIT_EXCEEDED",ErrorClass.DATA);
      Map<String,Object> request=new LinkedHashMap<>();
      if (vertical) {
        request.put("limit",pageSize);
        if(collection!=null) request.put("collection",collection);
        if(token!=null) {
          if("WATERMARK".equals(mode)) throw error("ING_WATERMARK_POSITION_UNSUPPORTED",ErrorClass.CONFIGURATION);
          request.put("SNAPSHOT".equals(mode)?"page":"cursor",token);
        }
      } else {
        request.put("pageSize",pageSize);
        if(token!=null) request.put("pageToken",token);
      }
      page=fetch(request);
      vertical=vertical || page.has("schemaVersion");
      if (gate!=null && (vertical || openIncident)) {
        JsonNode declaration=json.createObjectNode();
        String name=collection!=null ? collection : page.path("collection").textValue();
        if(vertical && name!=null && !name.isBlank()) {
          try { declaration=fetch(Map.of("resource","schema","collection",name)); }
          catch(AdapterException invalid) {
            if(!"ING_JSON_INVALID".equals(invalid.code())) throw invalid;
            // A malformed declaration provides no evidence of schema equality.
          }
        }
        var schema=PullSchema.compare(bundle,page,declaration);
        if(initialPosition.containsKey("bindingRef") && !bundle.bindingRef().equals(initialPosition.get("bindingRef"))
            || initialPosition.containsKey("collection") && !Objects.equals(initialPosition.get("collection"),page.path("collection").textValue())
            || initialPosition.containsKey("mode") && !Objects.equals(initialPosition.get("mode"),page.path("mode").textValue())) {
          var differences=new ArrayList<>(schema.differences());
          differences.add("CHECKPOINT_SCOPE_CHANGED");
          schema=new PullSchema.Comparison(schema.expected(),schema.observed(),List.copyOf(differences),schema.declaration());
        }
        gate.check(schema,pages==1 ? initialPosition : position());
      }
      items=page.at(itemsPointer);
      if (!items.isArray() || offset<0 || offset>items.size()) throw error("ING_JSON_ITEMS_INVALID",ErrorClass.DATA);
      if (vertical) {
        if ((long)read+items.size()-offset>maxRecords) throw error("ING_RECORD_LIMIT_EXCEEDED",ErrorClass.DATA);
        mode=page.path("mode").asText();
        next=page.path("next").textValue();
        if (!page.path("hasMore").isBoolean() || !Set.of("CURSOR","SNAPSHOT","WATERMARK").contains(mode))
          throw error("ING_VERTICAL_PAGE_INVALID",ErrorClass.DATA);
        more=page.get("hasMore").booleanValue();
        if(more && (next==null || next.isBlank() || next.equals(token))) throw error("ING_PAGE_TOKEN_INVALID",ErrorClass.DATA);
      } else {
        // Validate the remaining flat page before releasing any of its records.
        for(int i=offset;i<items.size();i++) {
          JsonNode item=items.get(i);
          if(!item.isObject()) throw error("ING_JSON_RECORD_INVALID",ErrorClass.DATA);
          Map<String,Object> row=json.convertValue(item,new TypeReference<>(){});
          if((long)read+legacyRows.size()>=maxRecords) throw error("ING_RECORD_LIMIT_EXCEEDED",ErrorClass.DATA);
          legacyRows.add(new SourceRecord(identity(bundle.sourceId(),typeCode,row,keys),ordinal+legacyRows.size(),
              row,Map.of("bindingRef",bundle.bindingRef(),"page",pages)));
        }
        next=page.at(nextPointer).textValue();
        more=next!=null && !next.isBlank();
      }
    }

    private SourceRecord verticalRecord(JsonNode item) {
      if (!item.isObject()) throw error("ING_JSON_RECORD_INVALID",ErrorClass.DATA);
      Map<String,Object> provenance=new LinkedHashMap<>();
      provenance.put("bindingRef",bundle.bindingRef());
      provenance.put("page",pages);
      JsonNode id=item.path("id"), rev=item.path("revision");
      String op=item.path("op").asText();
      if (!id.isTextual() || id.textValue().isBlank() || !rev.isIntegralNumber()
          || !Set.of("UPSERT","DELETE").contains(op) || ("UPSERT".equals(op) && !item.path("properties").isObject()))
        throw error("ING_VERTICAL_EVENT_INVALID",ErrorClass.DATA);
      Map<String,Object> row="DELETE".equals(op)?Map.of():json.convertValue(item.get("properties"),new TypeReference<>(){});
      provenance.put("schemaVersion",page.path("schemaVersion").asText());
      provenance.put("id",id.textValue());
      provenance.put("sourceRevision",rev.asText());
      provenance.put("operation",op);
      if(item.path("changedAt").isTextual()) provenance.put("observedAt",item.get("changedAt").textValue());
      return new SourceRecord(identity(bundle.sourceId(),typeCode,Map.of("id",id.textValue()),List.of("id")),ordinal,row,provenance);
    }

    private JsonNode fetch(Map<String,Object> parameters) {
      byte[] bytes;
      try { bytes=gateway.fetch(bundle.bindingRef(),parameters,correlation); }
      catch(RuntimePorts.GatewayFailure e){throw new AdapterException(e.safeCode(),e.errorClass(),e.safeCode(),e.retryAfter().orElse(null));}
      if(bytes.length>maxBytes) throw error("ING_PAGE_TOO_LARGE",ErrorClass.DATA);
      try {
        JsonNode result=json.readTree(bytes);
        if(result==null || (vertical && !result.isObject())) throw error("ING_JSON_INVALID",ErrorClass.DATA);
        return result;
      } catch(java.io.IOException e){throw error("ING_JSON_INVALID",ErrorClass.DATA);}
    }

    private Map<String,Object> position() {
      Map<String,Object> cp=new LinkedHashMap<>();
      if(token!=null) cp.put("pageToken",token);
      cp.put("pageOffset",offset);
      if(vertical) {
        cp.put("recordIndex",ordinal);
        cp.put("mode",mode);
        cp.put("bindingRef",bundle.bindingRef());
        if(collection!=null) cp.put("collection",collection);
        else if(page!=null && page.path("collection").isTextual()) cp.put("collection",page.get("collection").textValue());
      } else {
        cp.put("recordIndex",read);
      }
      return cp;
    }
    public Checkpoint checkpoint() {
      var cp=position();
      if(vertical && items!=null && offset==items.size() && next!=null && !next.isBlank()) {
        cp.put("pageToken",next);
        cp.put("pageOffset",0);
      }
      return new Checkpoint(cp);
    }
    public void close(){done=true;legacyRows.clear();items=null;page=null;}
  }
}
