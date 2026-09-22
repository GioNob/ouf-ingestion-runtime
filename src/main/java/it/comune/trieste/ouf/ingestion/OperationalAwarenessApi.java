package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.web.bind.annotation.*;

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
  public record ExplainQuery(UUID incidentId){}
  private static int limit(Query q){return q==null||q.limit()==null?50:q.limit();}

  @PostMapping("/status")
  Map<String,Object> status(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.owner(request,"ingestion.operations.read");return service.envelope("ingestion.operations.read",service.status(q==null?null:q.sourceId(),limit(q),actor),actor);}

  @PostMapping("/history")
  Map<String,Object> history(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.owner(request,"ingestion.operations.read");return service.envelope("ingestion.operations.read",service.history(q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor),actor);}

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
