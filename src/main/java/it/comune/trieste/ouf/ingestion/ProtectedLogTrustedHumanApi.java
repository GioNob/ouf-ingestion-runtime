package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController @RequestMapping("/api/trusted-human/v1/ingestion/logs")
public class ProtectedLogTrustedHumanApi {
  private final ProtectedOperationalLogService logs;private final TrustedAuthorizationContext authorization;
  public ProtectedLogTrustedHumanApi(ProtectedOperationalLogService logs,TrustedAuthorizationContext authorization){this.logs=logs;this.authorization=authorization;}
  public record Query(OffsetDateTime from,OffsetDateTime to,String severity,String correlationId,UUID runId,String sourceId,Integer limit,String purpose){}
  public record Aggregate(Query query,String dimension){}
  @PostMapping("/search") Map<String,Object> search(@RequestBody Query q,@RequestHeader HttpHeaders headers,HttpServletRequest request){String c=correlation(headers);try{return logs.search(toSearch(q),authorization.require(request,"ingestion.log.read",true),c);}catch(ResponseStatusException denied){logs.denied(request,"SEARCH",q.purpose(),c);throw denied;}}
  @PostMapping("/aggregate") Map<String,Object> aggregate(@RequestBody Aggregate q,@RequestHeader HttpHeaders headers,HttpServletRequest request){String c=correlation(headers);try{return Map.of("items",logs.aggregate(toSearch(q.query()),q.dimension(),authorization.require(request,"ingestion.log.aggregate",true),c));}catch(ResponseStatusException denied){logs.denied(request,"AGGREGATE",q.query()==null?null:q.query().purpose(),c);throw denied;}}
  private static ProtectedOperationalLogService.Search toSearch(Query q){if(q==null)throw new IllegalArgumentException("ING_LOG_QUERY_REQUIRED");return new ProtectedOperationalLogService.Search(q.from(),q.to(),q.severity(),q.correlationId(),q.runId(),q.sourceId(),q.limit(),q.purpose());}
  private static String correlation(HttpHeaders h){return Optional.ofNullable(h.getFirst("X-Correlation-ID")).filter(x->!x.isBlank()).orElseGet(()->UUID.randomUUID().toString());}
}
