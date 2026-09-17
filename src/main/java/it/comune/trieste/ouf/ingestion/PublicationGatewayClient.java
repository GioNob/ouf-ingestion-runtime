package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name="ouf.ingestion.activation.enabled",havingValue="true")
public class PublicationGatewayClient implements RuntimePorts.ActiveBundlePort,RuntimePorts.SemanticPort {
 private final ObjectMapper json;private final URI gateway;private final Path tokenFile;private final String tenant;private final HttpClient http;
 public PublicationGatewayClient(ObjectMapper json,@Value("${ouf.ingestion.activation.gateway-url}") String gateway,@Value("${ouf.ingestion.activation.token-file}") String token,@Value("${ouf.ingestion.activation.tenant-id}") String tenant){
  this.json=json;this.gateway=URI.create(gateway);this.tokenFile=Path.of(token);this.tenant=tenant;
  if(!Set.of("http","https").contains(this.gateway.getScheme())||this.gateway.getHost()==null||this.gateway.getUserInfo()!=null||this.gateway.getQuery()!=null||this.gateway.getFragment()!=null||!Set.of("","/").contains(this.gateway.getPath())||tenant.isBlank())throw new IllegalArgumentException("ING_ACTIVATION_GATEWAY_INVALID");
  http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).followRedirects(HttpClient.Redirect.NEVER).build();
 }
 public Page discover(String after){Map<String,Object> page=get("/api/onboarding/v1/runtime/publications?limit=20&after="+escape(after));Object rows=page.get("items");if(!(rows instanceof List<?> list)||list.size()>20||!(page.get("nextAfter") instanceof String cursor)||cursor.length()>160||(!cursor.isEmpty()&&cursor.compareTo(after)<=0))throw new IllegalArgumentException("ING_PUBLICATION_PAGE_INVALID");var items=new ArrayList<Map<String,Object>>();for(Object row:list){if(!(row instanceof Map<?,?> raw))throw new IllegalArgumentException("ING_PUBLICATION_PAGE_INVALID");@SuppressWarnings("unchecked")var item=(Map<String,Object>)raw;items.add(item);}return new Page(items,cursor);}
 public PublishedActivation decode(Map<String,Object> row){return PublishedActivation.decode(row,tenant,json);}
 @Override public ExecutionBundle loadAndVerify(String source){return active(source).execution();}
 @Override public ExecutionBundle loadAndVerify(RunStateRepository.ScheduleClaim claim){var a=active(claim.sourceId());if(!a.enabled()||claim.publicationId()==null||!a.publicationId().equals(claim.publicationId())||!a.execution().checksum().equals(claim.publicationChecksum()))throw new IllegalArgumentException("ING_ACTIVE_PUBLICATION_CHANGED");return a.execution();}
 private PublishedActivation active(String source){return decode(get("/api/onboarding/v1/runtime/publications/"+escape(source)+"/active"));}
 @Override public void preflight(Collection<String> refs){throw new IllegalArgumentException("ING_EXACT_SEMANTIC_BINDINGS_REQUIRED");}
 @Override public void preflight(ExecutionBundle bundle){
  var published=PublishedActivation.map(bundle.configuration(),"publishedBundle");Object raw=published.get("semanticReferenceBindings");if(!(raw instanceof List<?> bindings)||bindings.isEmpty()||bindings.size()>20)throw new IllegalArgumentException("ING_EXACT_SEMANTIC_BINDINGS_REQUIRED");
  var expected=new HashSet<>((Collection<?>)bundle.configuration().get("pinnedReferences"));var seen=new HashSet<String>();
  for(Object item:bindings){if(!(item instanceof Map<?,?>))throw new IllegalArgumentException("ING_EXACT_SEMANTIC_BINDINGS_REQUIRED");@SuppressWarnings("unchecked")var b=(Map<String,Object>)item;
   String id=PublishedActivation.text(b,"semanticId"),version=PublishedActivation.text(b,"semanticVersion"),revision=UUID.fromString(PublishedActivation.text(b,"revisionId")).toString(),publication=UUID.fromString(PublishedActivation.text(b,"publicationSetId")).toString();String ref=id+"@"+version;if(!expected.contains(ref)||!seen.add(ref))throw new IllegalArgumentException("ING_SEMANTIC_BINDING_MISMATCH");
   var resolved=get("/api/semantic/v1/references:resolve?semanticId="+escape(id)+"&revisionId="+revision+"&publicationSetId="+publication);
   if(!id.equals(resolved.get("semantic_id"))||!version.equals(resolved.get("semantic_version"))||!revision.equals(resolved.get("revision_id"))||!publication.equals(resolved.get("publication_set_id"))||!"ACTIVE".equals(resolved.get("status")))throw new IllegalArgumentException("ING_SEMANTIC_BINDING_MISMATCH");
  }
  if(!seen.equals(expected))throw new IllegalArgumentException("ING_SEMANTIC_BINDING_MISMATCH");
 }
 @SuppressWarnings("unchecked") private Map<String,Object> get(String path){
  try{String token=Files.readString(tokenFile).strip();if(token.isEmpty()||token.length()>16384||token.chars().anyMatch(Character::isWhitespace))throw new IllegalArgumentException("ING_WORKLOAD_TOKEN_INVALID");
   var request=HttpRequest.newBuilder(gateway.resolve(path)).timeout(Duration.ofSeconds(4)).header("Authorization","Bearer "+token).header("Accept","application/json").header("X-Correlation-ID",UUID.randomUUID().toString()).GET().build();var response=http.send(request,HttpResponse.BodyHandlers.ofInputStream());
   try(var body=response.body()){byte[] bytes=body.readNBytes(2097153);if(bytes.length>2097152)throw new IllegalArgumentException("ING_PUBLICATION_RESPONSE_TOO_LARGE");if(response.statusCode()!=200)throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_GATEWAY_"+response.statusCode(),response.statusCode()==401||response.statusCode()==403?AdapterSpi.ErrorClass.AUTH_ROUTE:response.statusCode()>=500||response.statusCode()==429?AdapterSpi.ErrorClass.TRANSIENT_SOURCE:AdapterSpi.ErrorClass.CONFIGURATION);return json.readValue(bytes,Map.class);}
  }catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_INTERRUPTED",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);}catch(IOException e){throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_IO_FAILED",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);}
 }
 private static String escape(String s){return URLEncoder.encode(s,java.nio.charset.StandardCharsets.UTF_8);}
 public record Page(List<Map<String,Object>> items,String nextAfter){}
}
