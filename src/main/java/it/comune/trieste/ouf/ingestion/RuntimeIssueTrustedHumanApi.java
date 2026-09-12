package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/trusted-human/v1/ingestion/issues")
public class RuntimeIssueTrustedHumanApi {
  private final RuntimeIssueService issues;private final TrustedAuthorizationContext authorization;
  public RuntimeIssueTrustedHumanApi(RuntimeIssueService issues,TrustedAuthorizationContext authorization){this.issues=issues;this.authorization=authorization;}
  public record CloseRequest(String reason,long expectedVersion){}public record ReviewRequest(long expectedVersion){}
  @GetMapping Map<String,Object> search(@RequestParam(required=false)String state,@RequestParam(required=false)String severity,@RequestParam(required=false)String sourceId,@RequestParam(required=false)UUID runId,@RequestParam(defaultValue="50")int limit,HttpServletRequest request){return Map.of("items",issues.search(state,severity,sourceId,runId,limit,authorization.require(request,"ouf.ingestion.issue.search",false)));}
  @GetMapping("/{id}") Map<String,Object> get(@PathVariable UUID id,HttpServletRequest request){return issues.get(id,authorization.require(request,"ouf.ingestion.issue.read",false));}
  @GetMapping("/schema-observations") Map<String,Object> observations(@RequestParam(required=false)String sourceId,@RequestParam(required=false)String outcome,@RequestParam(defaultValue="50")int limit,HttpServletRequest request){return Map.of("items",issues.observations(sourceId,outcome,limit,authorization.require(request,"ouf.ingestion.issue.search",false)));}
  @PostMapping("/{id}/resolve") ResponseEntity<Void> resolve(@PathVariable UUID id,@RequestBody CloseRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){issues.close(id,"RESOLVED",body.reason(),body.expectedVersion(),authorization.require(request,"ouf.ingestion.issue.resolve",true),correlation(correlation));return ResponseEntity.noContent().build();}
  @PostMapping("/{id}/dismiss") ResponseEntity<Void> dismiss(@PathVariable UUID id,@RequestBody CloseRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){issues.close(id,"DISMISSED",body.reason(),body.expectedVersion(),authorization.require(request,"ouf.ingestion.issue.dismiss",true),correlation(correlation));return ResponseEntity.noContent().build();}
  @PostMapping("/{id}/onboarding-review") ResponseEntity<Map<String,Object>> review(@PathVariable UUID id,@RequestBody ReviewRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){String c=correlation(correlation);UUID requestId=issues.requestOnboardingReview(id,body.expectedVersion(),authorization.require(request,"ouf.ingestion.onboarding-review.request",false),c);return ResponseEntity.accepted().body(Map.of("requestId",requestId,"status","QUEUED","correlationId",c));}
  private static String correlation(String c){return c==null||c.isBlank()?UUID.randomUUID().toString():c;}
}
