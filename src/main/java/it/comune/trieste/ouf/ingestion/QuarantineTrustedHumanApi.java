package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/trusted-human/v1/ingestion/quarantine")
public class QuarantineTrustedHumanApi {
  private final QuarantineService service;private final ReplayRepository replays;private final TrustedAuthorizationContext authorization;
  public QuarantineTrustedHumanApi(QuarantineService service,ReplayRepository replays,TrustedAuthorizationContext authorization){this.service=service;this.replays=replays;this.authorization=authorization;}
  public record ReplayRequest(String targetBundleRef){}public record CloseRequest(String reason,String supersededBy,long expectedVersion){}
  @GetMapping Map<String,Object> search(@RequestParam(defaultValue="OPEN") String state,@RequestParam(defaultValue="50") int limit,HttpServletRequest request){return Map.of("items",service.search(state,limit,authorization.require(request,"ingestion.quarantine.read",true)));}
  @GetMapping("/{id}") Map<String,Object> get(@PathVariable UUID id,HttpServletRequest request){return service.get(id,authorization.require(request,"ingestion.quarantine.read",true));}
  @GetMapping("/{id}/preview") Map<String,Object> preview(@PathVariable UUID id,HttpServletRequest request){return service.preview(id,authorization.require(request,"ouf.ingestion.quarantine.inspect",false));}
  @PostMapping("/{id}/replay") ResponseEntity<Map<String,Object>> replay(@PathVariable UUID id,@RequestBody ReplayRequest body,@RequestHeader(value="X-Correlation-ID",required=false) String correlation,HttpServletRequest request){var actor=authorization.require(request,"ingestion.quarantine.replay",true);String c=correlation==null||correlation.isBlank()?UUID.randomUUID().toString():correlation;UUID replay=service.requestReplay(id,body.targetBundleRef(),actor,c);return ResponseEntity.accepted().body(Map.of("replayId",replay,"status","QUEUED","correlationId",c));}
  @PostMapping("/{id}/reprocess") ResponseEntity<Map<String,Object>> reprocess(@PathVariable UUID id,@RequestBody ReplayRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){String c=correlation(correlation);UUID replay=service.requestReplay(id,body.targetBundleRef(),authorization.require(request,"ouf.ingestion.quarantine.reprocess",true),c);return ResponseEntity.accepted().body(Map.of("replayId",replay,"status","QUEUED","correlationId",c));}
  @PostMapping("/{id}/dismiss") ResponseEntity<Void> dismiss(@PathVariable UUID id,@RequestBody CloseRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){service.close(id,"DISMISSED",body.reason(),null,body.expectedVersion(),authorization.require(request,"ouf.ingestion.issue.dismiss",true),correlation(correlation));return ResponseEntity.noContent().build();}
  @PostMapping("/{id}/supersede") ResponseEntity<Void> supersede(@PathVariable UUID id,@RequestBody CloseRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){service.close(id,"SUPERSEDED",body.reason(),body.supersededBy(),body.expectedVersion(),authorization.require(request,"ouf.ingestion.issue.dismiss",true),correlation(correlation));return ResponseEntity.noContent().build();}
  @GetMapping("/replays/{replayId}") Map<String,Object> replayStatus(@PathVariable UUID replayId,HttpServletRequest request){authorization.require(request,"ingestion.replay.read",true);return replays.view(replayId);}
  private static String correlation(String c){return c==null||c.isBlank()?UUID.randomUUID().toString():c;}
}
