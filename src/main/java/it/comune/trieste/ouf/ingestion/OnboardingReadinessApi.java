package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.authorization.*;
import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
  @ExceptionHandler(AdapterSpi.AdapterException.class)
  ResponseEntity<Map<String,String>> adapterFailure(AdapterSpi.AdapterException failure){
    HttpStatus status=switch(failure.errorClass()){
      case DATA, CONFIGURATION -> HttpStatus.UNPROCESSABLE_ENTITY;
      case AUTH_ROUTE -> HttpStatus.FORBIDDEN;
      case TRANSIENT_SOURCE -> HttpStatus.SERVICE_UNAVAILABLE;
      case PROGRAMMING -> HttpStatus.INTERNAL_SERVER_ERROR;
    };
    return problem(status,failure.code());
  }
  @ExceptionHandler(FrozenContractValidator.ContractViolation.class)
  ResponseEntity<Map<String,String>> contractFailure(FrozenContractValidator.ContractViolation failure){
    return problem("ING_CONTRACT_INVALID".equals(failure.safeCode())?HttpStatus.UNPROCESSABLE_ENTITY:HttpStatus.INTERNAL_SERVER_ERROR,failure.safeCode());
  }
  private static ResponseEntity<Map<String,String>> problem(HttpStatus status,String code){
    return ResponseEntity.status(status).body(Map.of("code",code!=null && code.matches("ING_[A-Z0-9_]{1,76}")?code:"ING_READINESS_UNPROVEN"));
  }
  @PostMapping public Map<String,Object> verify(@RequestBody Request body,HttpServletRequest request){
    if(body==null||body.sourceId()==null||!body.sourceId().matches("[A-Za-z0-9_.:-]{1,160}")||body.onboardingVersionId()==null
        ||body.configurationHash()==null||!body.configurationHash().matches("sha256:[a-f0-9]{64}"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"ING_READINESS_IDENTITY_REQUIRED");
    var owner=OwnerAuthorization.bind(request);
    if(owner.principal().actorType()!=PrincipalContext.ActorType.HUMAN||!tenant.equals(owner.principal().tenantId()))throw new SecurityException("ING_READINESS_HUMAN_TENANT_REQUIRED");
    Runnable authorize=()->owner.require("ouf.onboarding.configuration.write",new ResourceContext("source",body.sourceId(),tenant,null,Map.of("onboardingVersionId",body.onboardingVersionId().toString())));
    authorize.run();
    return service.verify(body.sourceId(),body.onboardingVersionId(),body.configurationHash(),authorize);
  }
  @ExceptionHandler(RuntimePorts.GatewayFailure.class)
  ResponseEntity<Map<String,String>> gatewayFailure(RuntimePorts.GatewayFailure failure){
    return problem(switch(failure.errorClass()){
      case TRANSIENT_SOURCE -> HttpStatus.SERVICE_UNAVAILABLE;
      case AUTH_ROUTE -> HttpStatus.FORBIDDEN;
      case PROGRAMMING -> HttpStatus.INTERNAL_SERVER_ERROR;
      default -> HttpStatus.UNPROCESSABLE_ENTITY;
    },failure.safeCode());
  }
  @ExceptionHandler(ValuePins.BindingFailure.class)
  ResponseEntity<Map<String,Object>> invalidReference(ValuePins.BindingFailure failure){
    var pin=failure.detail();
    var problem=new LinkedHashMap<>(pin);
    problem.put("code",failure.getMessage());
    problem.put("semanticReference",pin);
    return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(problem);
  }
  @ExceptionHandler(IllegalArgumentException.class)
  ResponseEntity<Map<String,String>> invalidConfiguration(IllegalArgumentException failure){
    return problem(HttpStatus.UNPROCESSABLE_ENTITY,failure.getMessage());
  }
  @ExceptionHandler(ConfiguredValueTransform.InvalidValue.class)
  ResponseEntity<Map<String,Object>> invalidValue(ConfiguredValueTransform.InvalidValue failure){
    return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of("code",failure.getMessage(),"rowOrdinal",failure.rowOrdinal()));
  }
}
