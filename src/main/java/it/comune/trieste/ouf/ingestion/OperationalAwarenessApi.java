package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/internal/v1/ingestion/operations")
public class OperationalAwarenessApi {
  private final OperationalAwarenessService service;
  private final RuntimeIssueService issues;
  @org.springframework.beans.factory.annotation.Autowired private IncidentTimeline timeline;
  private final TrustedAuthorizationContext authorization;

  public OperationalAwarenessApi(OperationalAwarenessService service,RuntimeIssueService issues,TrustedAuthorizationContext authorization){
    this.service=service;this.issues=issues;this.authorization=authorization;
  }
  public record Query(String sourceId,String state,OffsetDateTime since,Integer limit){}
  public record HistoryQuery(String sourceId,OffsetDateTime since,OffsetDateTime until,Integer limit){}
  public record ExplainQuery(UUID incidentId){}
  private static int limit(Query q){return q==null||q.limit()==null?50:q.limit();}

  @PostMapping("/status")
  Map<String,Object> status(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.owner(request,"ingestion.operations.read");return service.envelope("ingestion.operations.read",service.status(q==null?null:q.sourceId(),limit(q),actor),actor);}

  @PostMapping("/history")
  Map<String,Object> history(@RequestBody(required=false)HistoryQuery q,HttpServletRequest request){
    var actor=authorization.owner(request,"ingestion.operations.read");
    var now=OffsetDateTime.now(ZoneOffset.UTC);
    var until=q==null||q.until()==null?now:q.until();
    var since=q==null||q.since()==null?until.minusDays(1):q.since();
    int pageSize=q==null||q.limit()==null?50:q.limit();
    String source=q==null?null:q.sourceId();
    if(pageSize<1||pageSize>200||(source!=null&&(source.isBlank()||source.length()>200))||
       since.isAfter(until)||until.isAfter(now.plusSeconds(1))||Duration.between(since,until).compareTo(Duration.ofDays(30))>0)
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"ING_HISTORY_QUERY_INVALID");
    if(source!=null)service.requireVisible("ingestion.operations.read",Map.of("source_ref",source),actor);
    var out=new LinkedHashMap<>(service.envelope("ingestion.operations.read",service.history(source,since,until,pageSize,actor),actor));
    out.put("since",since.toString());out.put("until",until.toString());
    return out;
  }

  @PostMapping("/incidents")
  Map<String,Object> incidents(@RequestBody(required=false)IncidentTimeline.Query q,HttpServletRequest request){var actor=authorization.owner(request,"operations.incident.read");return timeline.page(q,actor);}

  @PostMapping("/incidents/explain")
  Map<String,Object> explain(@RequestBody ExplainQuery q,HttpServletRequest request){
    if(q==null||q.incidentId()==null)throw new IllegalArgumentException("ING_OPERATIONAL_INCIDENT_ID_REQUIRED");
    var actor=authorization.owner(request,"operations.incident.explain");
    var persisted=timeline.explain(q.incidentId(),actor);if(persisted.isPresent())return persisted.orElseThrow();
    var row=issues.get(q.incidentId(),actor);
    service.requireVisible("operations.incident.explain",row,actor);
    var result=issues.explain(q.incidentId(),actor);result.remove("evidenceRef");result.remove("quarantineId");return result;
  }

  @PostMapping("/summary")
  Map<String,Object> summary(@RequestBody(required=false)Query q,HttpServletRequest request){
    var actor=authorization.owner(request,"operations.status.read");
    var now=OffsetDateTime.now(java.time.ZoneOffset.UTC);var since=q==null||q.since()==null?now.minusDays(1):q.since();
    String source=q==null?null:q.sourceId();
    if(limit(q)<1||limit(q)>100||(q!=null&&q.state()!=null)||(source!=null&&(source.isBlank()||source.length()>200))||since.isAfter(now)||since.isBefore(now.minusDays(30)))throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.BAD_REQUEST,"INVALID_SUMMARY_QUERY");
    if(source!=null)service.requireVisible("operations.status.read",Map.of("source_ref",source),actor);
    return service.summary(source,since,limit(q),actor);
  }
}
