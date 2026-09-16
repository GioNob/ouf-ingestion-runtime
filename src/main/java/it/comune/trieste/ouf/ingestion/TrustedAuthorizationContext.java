package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.authorization.ServletAuthorization;
import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
public class TrustedAuthorizationContext {
  public static final String ACTOR_TYPE="ouf.actorType",TENANT="ouf.tenantId",CAPABILITIES="ouf.capabilities",DECISION="ouf.authorizationDecisionRef";
  public Context resolve(HttpServletRequest request){
    try{var c=ServletAuthorization.resolve(request);return new Context(c.principal().subjectId(),c.principal().actorType().name(),c.principal().tenantId(),c.capabilities(),c.decisionRef());}
    catch(SecurityException e){throw new ResponseStatusException(HttpStatus.FORBIDDEN,e.getMessage());}
  }
  public Context require(HttpServletRequest request,String capability,boolean human){Context c=resolve(request);if(human&&!"HUMAN".equals(c.actorType()))throw new ResponseStatusException(HttpStatus.FORBIDDEN,"ING_HUMAN_USER_REQUIRED");if(!c.capabilities().contains(capability))throw new ResponseStatusException(HttpStatus.FORBIDDEN,"ING_CAPABILITY_REQUIRED");return c;}
  public record Context(String subject,String actorType,String tenantId,Set<String> capabilities,String decisionRef){}
}
