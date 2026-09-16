package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/internal/v1/ingestion/operations")
public class OperationalAwarenessApi {
  private final OperationalAwarenessService service;private final TrustedAuthorizationContext authorization;
  public OperationalAwarenessApi(OperationalAwarenessService service,TrustedAuthorizationContext authorization){this.service=service;this.authorization=authorization;}
  public record Query(String sourceId,String state,OffsetDateTime since,Integer limit){}
  private static int limit(Query q){return q==null||q.limit()==null?50:q.limit();}

  @PostMapping("/status")
  Map<String,Object> status(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.require(request,"ingestion.operations.read",false);return Map.of("items",service.status(q==null?null:q.sourceId(),limit(q),actor),"partial",false);}

  @PostMapping("/history")
  Map<String,Object> history(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.require(request,"ingestion.operations.read",false);return Map.of("items",service.history(q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor),"partial",false);}

  @PostMapping("/incidents")
  Map<String,Object> incidents(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.require(request,"operations.incident.read",false);return Map.of("items",service.incidents(q==null?null:q.state(),q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor),"partial",false);}

  @PostMapping("/summary")
  Map<String,Object> summary(@RequestBody(required=false)Query q,HttpServletRequest request){var actor=authorization.require(request,"operations.status.read",false);return service.summary(q==null?null:q.sourceId(),q==null?null:q.since(),limit(q),actor);}
}
