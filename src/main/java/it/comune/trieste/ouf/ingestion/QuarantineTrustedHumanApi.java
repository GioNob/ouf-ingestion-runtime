package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/trusted-human/v1/ingestion/quarantine")
public class QuarantineTrustedHumanApi {
  private final QuarantineService service;private final TrustedAuthorizationContext authorization;
  public QuarantineTrustedHumanApi(QuarantineService service,TrustedAuthorizationContext authorization){this.service=service;this.authorization=authorization;}
  public record ReplayRequest(String targetBundleRef){}
  @GetMapping Map<String,Object> search(@RequestParam(defaultValue="OPEN") String state,@RequestParam(defaultValue="50") int limit,HttpServletRequest request){authorization.require(request,"ingestion.quarantine.read",true);return Map.of("items",service.search(state,limit));}
  @GetMapping("/{id}") Map<String,Object> get(@PathVariable UUID id,HttpServletRequest request){authorization.require(request,"ingestion.quarantine.read",true);return service.get(id);}
  @PostMapping("/{id}/replay") ResponseEntity<Map<String,Object>> replay(@PathVariable UUID id,@RequestBody ReplayRequest body,@RequestHeader(value="X-Correlation-ID",required=false) String correlation,HttpServletRequest request){var actor=authorization.require(request,"ingestion.quarantine.replay",true);String c=correlation==null||correlation.isBlank()?UUID.randomUUID().toString():correlation;UUID replay=service.requestReplay(id,body.targetBundleRef(),actor,c);return ResponseEntity.accepted().body(Map.of("replayId",replay,"status","QUEUED","correlationId",c));}
}
