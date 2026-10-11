package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
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

  public OperationalAwarenessApi(OperationalAwarenessService service,RuntimeIssueService issues,TrustedAuthorizationContext authorization){
    this(service,issues,authorization,null);
  }
  @Autowired public OperationalAwarenessApi(OperationalAwarenessService service,RuntimeIssueService issues,TrustedAuthorizationContext authorization,IncidentSnapshotService snapshots){
    this.service=service;this.issues=issues;this.authorization=authorization;this.snapshots=snapshots;
  }
  public record Query(String sourceId,String state,OffsetDateTime since,Integer limit,Boolean snapshot,UUID cursor){
    public Query(String sourceId,String state,OffsetDateTime since,Integer limit){this(sourceId,state,since,limit,null,null);}
  }
  public record ExplainQuery(UUID incidentId){}
  private static void nonPaged(Query q){if(q!=null&&(q.cursor()!=null||Boolean.TRUE.equals(q.snapshot())))throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.BAD_REQUEST,"ING_CURSOR_UNSUPPORTED");}
  private static int limit(Query q){return q==null||q.limit()==null?50:q.limit();}

  @PostMapping("/status")
  Map<String,Object> status(@RequestBody(required=false)Query q,HttpServletRequest request){nonPaged(q);var actor=authorization.owner(request,"ingestion.operations.read");return service.envelope("ingestion.operations.read",service.status(q==null?null:q.sourceId(),limit(q),actor),actor);}

  @PostMapping("/history")
  Map<String,Object> history(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.owner(request,"ingestion.operations.read");return service.envelope("ingestion.operations.read",service.history(q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor),actor);}

  @PostMapping("/incidents")
  Map<String,Object> incidents(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.owner(request,"operations.incident.read");if(q!=null&&(Boolean.TRUE.equals(q.snapshot())||q.cursor()!=null)){if(snapshots==null)throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_UNAVAILABLE");return snapshots.page(q.sourceId(),q.state(),q.since(),q.limit(),q.cursor(),actor);}return service.envelope("operations.incident.read",service.incidents(q==null?null:q.state(),q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor),actor);}

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
