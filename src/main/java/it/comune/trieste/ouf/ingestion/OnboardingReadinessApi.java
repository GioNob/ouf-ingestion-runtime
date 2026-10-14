package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.authorization.*;
import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** HUMAN intent remains with the source owner; this endpoint can only prove compatibility. */
@RestController
@ConditionalOnProperty(name={"ouf.ingestion.activation.enabled","ouf.ingestion.execution.enabled"},havingValue="true")
@RequestMapping("/api/trusted-human/v1/ingestion/onboarding-readiness")
public class OnboardingReadinessApi {
  private final OnboardingReadinessService service;private final String tenant;
  public OnboardingReadinessApi(OnboardingReadinessService service,@Value("${ouf.ingestion.activation.tenant-id}") String tenant){this.service=service;this.tenant=tenant;}
  public record Request(String sourceId,UUID onboardingVersionId,String configurationHash){}
  @PostMapping public Map<String,Object> verify(@RequestBody Request body,HttpServletRequest request){
    if(body==null||body.sourceId()==null||!body.sourceId().matches("[A-Za-z0-9_.:-]{1,160}")||body.onboardingVersionId()==null
        ||body.configurationHash()==null||!body.configurationHash().matches("sha256:[a-f0-9]{64}"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"ING_READINESS_IDENTITY_REQUIRED");
    var owner=OwnerAuthorization.bind(request);
    if(owner.principal().actorType()!=PrincipalContext.ActorType.HUMAN||!tenant.equals(owner.principal().tenantId()))throw new SecurityException("ING_READINESS_HUMAN_TENANT_REQUIRED");
    Runnable authorize=()->owner.require("ouf.onboarding.configuration.write",new ResourceContext("source",body.sourceId(),tenant,null,Map.of("onboardingVersionId",body.onboardingVersionId().toString())));
    authorize.run();
    try{return service.verify(body.sourceId(),body.onboardingVersionId(),body.configurationHash(),authorize);}
    catch(RuntimePorts.GatewayFailure failure){throw new ResponseStatusException(failure.errorClass()==AdapterSpi.ErrorClass.TRANSIENT_SOURCE?HttpStatus.SERVICE_UNAVAILABLE:failure.errorClass()==AdapterSpi.ErrorClass.AUTH_ROUTE?HttpStatus.FORBIDDEN:HttpStatus.UNPROCESSABLE_ENTITY,failure.safeCode());}
    catch(IllegalArgumentException failure){String code=failure.getMessage();throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,code!=null&&code.matches("ING_[A-Z0-9_]{1,80}")?code:"ING_READINESS_UNPROVEN");}
  }
}
