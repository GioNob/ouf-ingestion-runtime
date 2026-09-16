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

  @GetMapping("/status")
  Map<String,Object> status(@RequestParam(required=false)String sourceId,@RequestParam(defaultValue="50")int limit,HttpServletRequest request){
    var actor=authorization.require(request,"ingestion.operations.read",false);return Map.of("items",service.status(sourceId,limit,actor),"partial",false);
  }

  @GetMapping("/history")
  Map<String,Object> history(@RequestParam(required=false)String sourceId,@RequestParam(required=false)OffsetDateTime since,@RequestParam(defaultValue="50")int limit,HttpServletRequest request){
    var actor=authorization.require(request,"ingestion.operations.read",false);return Map.of("items",service.history(sourceId,since,limit,actor),"partial",false);
  }

  @GetMapping("/incidents")
  Map<String,Object> incidents(@RequestParam(required=false)String state,@RequestParam(required=false)String sourceId,@RequestParam(required=false)OffsetDateTime since,@RequestParam(defaultValue="50")int limit,HttpServletRequest request){
    var actor=authorization.require(request,"operations.incident.read",false);return Map.of("items",service.incidents(state,sourceId,since,limit,actor),"partial",false);
  }

  @GetMapping("/summary")
  Map<String,Object> summary(@RequestParam(required=false)String sourceId,@RequestParam(required=false)OffsetDateTime since,@RequestParam(defaultValue="50")int limit,HttpServletRequest request){
    var actor=authorization.require(request,"operations.status.read",false);return service.summary(sourceId,since,limit,actor);
  }
}
