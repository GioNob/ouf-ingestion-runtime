package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Consumes server-established Authorization/Gateway attributes; never actor headers. */
@Component
public class TrustedAuthorizationContext {
  public static final String ACTOR_TYPE="ouf.actorType",TENANT="ouf.tenantId",CAPABILITIES="ouf.capabilities",DECISION="ouf.authorizationDecisionRef";
  public Context resolve(HttpServletRequest request){
    Principal principal=request.getUserPrincipal();
    String type=attribute(request,ACTOR_TYPE),tenant=attribute(request,TENANT),decision=attribute(request,DECISION);
    if(principal==null||type==null||tenant==null||decision==null)throw denied("ING_TRUST_CONTEXT_REQUIRED");
    Object raw=request.getAttribute(CAPABILITIES);Set<String> capabilities=new HashSet<>();if(raw instanceof Collection<?> c)c.forEach(x->capabilities.add(String.valueOf(x)));
    return new Context(principal.getName(),type,tenant,Set.copyOf(capabilities),decision);
  }
  public Context require(HttpServletRequest request,String capability,boolean human){Context c=resolve(request);if(human&&!"HUMAN_USER".equals(c.actorType()))throw denied("ING_HUMAN_USER_REQUIRED");if(!c.capabilities().contains(capability))throw denied("ING_CAPABILITY_REQUIRED");return c;}
  private static String attribute(HttpServletRequest r,String name){Object v=r.getAttribute(name);return v instanceof String s&&!s.isBlank()?s:null;}
  private static ResponseStatusException denied(String code){return new ResponseStatusException(HttpStatus.FORBIDDEN,code);}
  public record Context(String subject,String actorType,String tenantId,Set<String> capabilities,String decisionRef){}
}
