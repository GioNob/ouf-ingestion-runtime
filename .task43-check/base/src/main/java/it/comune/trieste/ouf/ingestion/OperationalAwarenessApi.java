package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;

@RestController
@RequestMapping("/api/internal/v1/ingestion/operations")
public class OperationalAwarenessApi {
  private final OperationalAwarenessService service;
  private final RuntimeIssueService issues;
  private final TrustedAuthorizationContext authorization;
  private final IncidentSnapshotService snapshots;
  private final IncidentPublicationBookmarkService bookmarks;

  public OperationalAwarenessApi(OperationalAwarenessService service,RuntimeIssueService issues,TrustedAuthorizationContext authorization){
    this(service,issues,authorization,null);
  }
  public OperationalAwarenessApi(OperationalAwarenessService service,RuntimeIssueService issues,TrustedAuthorizationContext authorization,IncidentSnapshotService snapshots){
    this(service,issues,authorization,snapshots,null);
  }
  @Autowired public OperationalAwarenessApi(OperationalAwarenessService service,RuntimeIssueService issues,TrustedAuthorizationContext authorization,IncidentSnapshotService snapshots,IncidentPublicationBookmarkService bookmarks){
    this.service=service;this.issues=issues;this.authorization=authorization;this.snapshots=snapshots;this.bookmarks=bookmarks;
  }
  @JsonDeserialize(using=QueryDecoder.class)
  public record Query(String sourceId,String state,OffsetDateTime since,Integer limit,Boolean snapshot,UUID cursor,Boolean coverage,UUID resumeFrom){
    public Query(String sourceId,String state,OffsetDateTime since,Integer limit){this(sourceId,state,since,limit,null,null,null,null);}
    public Query(String sourceId,String state,OffsetDateTime since,Integer limit,Boolean snapshot,UUID cursor){this(sourceId,state,since,limit,snapshot,cursor,null,null);}
    public Query{
      if(coverage!=null&&!Boolean.TRUE.equals(coverage)||snapshot!=null&&!Boolean.TRUE.equals(snapshot))
        throw new IllegalArgumentException("ING_OPERATIONAL_QUERY_INVALID");
      if(resumeFrom!=null&&(sourceId!=null||state!=null||since!=null||snapshot!=null||cursor!=null||coverage!=null))
        throw new IllegalArgumentException("ING_BOOKMARK_RESUME_FILTERS_IMMUTABLE");
      if(Boolean.TRUE.equals(coverage)){
        if(since!=null||resumeFrom!=null||cursor==null&&!Boolean.TRUE.equals(snapshot)||
           cursor!=null&&(sourceId!=null||state!=null||limit!=null||snapshot!=null))
          throw new IllegalArgumentException("ING_BOOKMARK_QUERY_INVALID");
      }
      if(cursor!=null&&(sourceId!=null||state!=null||since!=null||limit!=null||snapshot!=null))
        throw new IllegalArgumentException("ING_CURSOR_FILTERS_IMMUTABLE");
    }
  }
  /** Closed streaming decode: duplicate/unknown/null fields never reach owner I/O. */
  public static class QueryDecoder extends JsonDeserializer<Query>{
    @Override public Query deserialize(JsonParser parser,DeserializationContext context)throws IOException{
      if(parser.currentToken()!=JsonToken.START_OBJECT)throw JsonMappingException.from(parser,"ING_OPERATIONAL_QUERY_INVALID");
      var values=new HashMap<String,JsonNode>();
      var known=Set.of("sourceId","state","since","limit","snapshot","cursor","coverage","resumeFrom");
      while(parser.nextToken()!=JsonToken.END_OBJECT){
        if(parser.currentToken()!=JsonToken.FIELD_NAME)throw JsonMappingException.from(parser,"ING_OPERATIONAL_QUERY_INVALID");
        String name=parser.currentName();
        if(!known.contains(name)||values.containsKey(name))throw JsonMappingException.from(parser,"ING_OPERATIONAL_QUERY_INVALID");
        parser.nextToken();JsonNode value=parser.readValueAsTree();
        if(value==null||value.isNull())throw JsonMappingException.from(parser,"ING_OPERATIONAL_QUERY_INVALID");
        values.put(name,value);
      }
      try{
        String source=text(values,"sourceId"),state=text(values,"state"),since=text(values,"since");
        Integer limit=null;if(values.containsKey("limit")){
          var n=values.get("limit");if(!n.isIntegralNumber()||!n.canConvertToInt()||n.intValue()<1||n.intValue()>100)throw new IllegalArgumentException();
          limit=n.intValue();
        }
        return new Query(source,state,since==null?null:OffsetDateTime.parse(since),limit,flag(values,"snapshot"),
          token(values,"cursor"),flag(values,"coverage"),token(values,"resumeFrom"));
      }catch(RuntimeException invalid){throw JsonMappingException.from(parser,"ING_OPERATIONAL_QUERY_INVALID");}
    }
    private static String text(Map<String,JsonNode> values,String key){
      var value=values.get(key);if(value==null)return null;
      if(!value.isTextual()||value.textValue().isBlank()||value.textValue().length()>200)throw new IllegalArgumentException();
      return value.textValue();
    }
    private static Boolean flag(Map<String,JsonNode> values,String key){
      var value=values.get(key);if(value==null)return null;
      if(!value.isBoolean()||!value.booleanValue())throw new IllegalArgumentException();return true;
    }
    private static UUID token(Map<String,JsonNode> values,String key){
      String value=text(values,key);if(value==null)return null;UUID token=UUID.fromString(value);
      if(!token.toString().equals(value)||token.equals(new UUID(0,0)))throw new IllegalArgumentException();return token;
    }
  }
  public record ExplainQuery(UUID incidentId){}
  private static void nonPaged(Query q){if(q!=null&&(q.cursor()!=null||Boolean.TRUE.equals(q.snapshot())||q.coverage()!=null||q.resumeFrom()!=null))throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.BAD_REQUEST,"ING_CURSOR_UNSUPPORTED");}
  private static int limit(Query q){return q==null||q.limit()==null?50:q.limit();}

  @PostMapping("/status")
  Map<String,Object> status(@RequestBody(required=false)Query q,HttpServletRequest request){nonPaged(q);var actor=authorization.owner(request,"ingestion.operations.read");return service.envelope("ingestion.operations.read",service.status(q==null?null:q.sourceId(),limit(q),actor),actor);}

  @PostMapping("/history")
  Map<String,Object> history(@RequestBody(required=false)Query q,HttpServletRequest request){nonPaged(q);var actor=authorization.owner(request,"ingestion.operations.read");return service.envelope("ingestion.operations.read",service.history(q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor),actor);}

  @PostMapping("/incidents")
  Map<String,Object> incidents(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.owner(request,"operations.incident.read");if(q!=null&&(Boolean.TRUE.equals(q.coverage())||q.resumeFrom()!=null)){
    if(bookmarks==null)throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE,"ING_BOOKMARK_UNAVAILABLE");
    if(q.resumeFrom()!=null)return bookmarks.resume(q.resumeFrom(),q.limit(),actor);
    if(q.cursor()!=null)return bookmarks.page(q.cursor(),actor);
    return bookmarks.start(q.sourceId(),q.state(),q.limit(),actor);
  }if(q!=null&&(Boolean.TRUE.equals(q.snapshot())||q.cursor()!=null)){if(snapshots==null)throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_UNAVAILABLE");return snapshots.page(q.sourceId(),q.state(),q.since(),q.limit(),q.cursor(),actor);}return service.envelope("operations.incident.read",service.incidents(q==null?null:q.state(),q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor),actor);}

  @PostMapping("/incidents/explain")
  Map<String,Object> explain(@RequestBody ExplainQuery q,HttpServletRequest request){
    if(q==null||q.incidentId()==null)throw new IllegalArgumentException("ING_OPERATIONAL_INCIDENT_ID_REQUIRED");
    var actor=authorization.owner(request,"operations.incident.explain");var row=issues.get(q.incidentId(),actor);
    service.requireVisible("operations.incident.explain",row,actor);
    var result=issues.explain(q.incidentId(),actor);result.remove("evidenceRef");result.remove("quarantineId");return result;
  }

  @PostMapping("/summary")
  Map<String,Object> summary(@RequestBody(required=false)Query q,HttpServletRequest request){nonPaged(q);var actor=authorization.owner(request,"operations.status.read");return service.summary(q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor);}
}
