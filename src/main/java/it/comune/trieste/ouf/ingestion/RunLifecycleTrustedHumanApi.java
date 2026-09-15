package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.web.bind.annotation.*;

@RestController @ConditionalOnBean(RunLifecycleService.class) @RequestMapping("/api/trusted-human/v1/ingestion/runs")
public class RunLifecycleTrustedHumanApi {
  private final RunLifecycleService service;private final TrustedAuthorizationContext authorization;
  public RunLifecycleTrustedHumanApi(RunLifecycleService service,TrustedAuthorizationContext authorization){this.service=service;this.authorization=authorization;}
  public record ControlRequest(long expectedVersion){}
  @GetMapping("/{id}") Map<String,Object> get(@PathVariable UUID id,HttpServletRequest request){return service.view(id,authorization.require(request,"ingestion.run.read",true));}
  @PostMapping("/{id}/pause") Map<String,Object> pause(@PathVariable UUID id,@RequestBody ControlRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){return service.pause(id,body.expectedVersion(),authorization.require(request,"ingestion.run.pause",true),correlation(correlation));}
  @PostMapping("/{id}/resume") Map<String,Object> resume(@PathVariable UUID id,@RequestBody ControlRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){return service.resume(id,body.expectedVersion(),authorization.require(request,"ingestion.run.resume",true),correlation(correlation));}
  @PostMapping("/{id}/abort") Map<String,Object> abort(@PathVariable UUID id,@RequestBody ControlRequest body,@RequestHeader(value="X-Correlation-ID",required=false)String correlation,HttpServletRequest request){return service.abort(id,body.expectedVersion(),authorization.require(request,"ingestion.run.abort",true),correlation(correlation));}
  private static String correlation(String value){return value==null||value.isBlank()?UUID.randomUUID().toString():value;}
}
