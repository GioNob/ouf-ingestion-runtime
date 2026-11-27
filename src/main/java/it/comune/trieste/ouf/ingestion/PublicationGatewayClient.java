package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name="ouf.ingestion.activation.enabled",havingValue="true")
public class PublicationGatewayClient implements RuntimePorts.ActiveBundlePort,RuntimePorts.SemanticPort,RuntimePorts.HistoricalBundlePort {
 private final ReferenceBatch references;
 private final GatewayPacing pacing;
 private static final int MAX_RESPONSE_BYTES=2*1024*1024;
 private static final int MAX_CONFIGURATION_RESPONSE_BYTES=3*1024*1024;
 private static final int MAX_PUBLICATION_PAGE_SIZE=20;
 private static final String RESPONSE_TOO_LARGE="ING_PUBLICATION_RESPONSE_TOO_LARGE";
 private final ObjectMapper json;private final URI gateway;private final Path tokenFile;private final String tenant;private final Transport transport;
 @Autowired
 public PublicationGatewayClient(ObjectMapper json,@Value("${ouf.ingestion.activation.gateway-url}") String gateway,@Value("${ouf.ingestion.activation.token-file}") String token,@Value("${ouf.ingestion.activation.tenant-id}") String tenant,GatewayPacing pacing){
  this(json,gateway,token,tenant,HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3))
      .followRedirects(HttpClient.Redirect.NEVER).build()::sendAsync,pacing);
 }
 PublicationGatewayClient(ObjectMapper json,String gateway,String token,String tenant,Transport transport){
  this(json,gateway,token,tenant,transport,new GatewayPacing(key->null));
 }
 PublicationGatewayClient(ObjectMapper json,String gateway,String token,String tenant){
  this(json,gateway,token,tenant,new GatewayPacing(key->null));
 }
 PublicationGatewayClient(ObjectMapper json,String gateway,String token,String tenant,Transport transport,GatewayPacing pacing){
  this.pacing=pacing;this.references=new ReferenceBatch(json,this,pacing);
  this.json=json;this.gateway=URI.create(gateway);this.tokenFile=Path.of(token);this.tenant=tenant;this.transport=transport;
  if(!Set.of("http","https").contains(this.gateway.getScheme())||this.gateway.getHost()==null||this.gateway.getUserInfo()!=null||this.gateway.getQuery()!=null||this.gateway.getFragment()!=null||!Set.of("","/").contains(this.gateway.getPath())||tenant.isBlank())throw new IllegalArgumentException("ING_ACTIVATION_GATEWAY_INVALID");
 }
 public Page discover(String after){
  String cursorQuery="&after="+escape(after);
  for(int limit=MAX_PUBLICATION_PAGE_SIZE;;limit/=2){
   Map<String,Object> page;
   try{
    page=getConfiguration("/api/onboarding/v1/runtime/publications?limit="+limit+cursorQuery);
   }catch(PublicationResponseTooLarge oversized){
    if(limit==1)throw oversized;
    continue;
   }
   Object rows=page.get("items");
   if(!(rows instanceof List<?> list)||list.size()>limit||!(page.get("nextAfter") instanceof String cursor)
       ||cursor.length()>160||(!cursor.isEmpty()&&cursor.compareTo(after)<=0))
    throw new IllegalArgumentException("ING_PUBLICATION_PAGE_INVALID");
   var items=new ArrayList<Map<String,Object>>();
   for(Object row:list){
    if(!(row instanceof Map<?,?> raw))throw new IllegalArgumentException("ING_PUBLICATION_PAGE_INVALID");
    @SuppressWarnings("unchecked")var item=(Map<String,Object>)raw;
    items.add(item);
   }
   return new Page(items,cursor);
  }
 }
 public PublishedActivation decode(Map<String,Object> row){return PublishedActivation.decode(row,tenant,json);}
 @Override public ExecutionBundle loadAndVerify(String source){return active(source).execution();}
 @Override public ExecutionBundle loadAndVerify(RunStateRepository.ScheduleClaim claim){var a=active(claim.sourceId());if(!a.enabled()||claim.publicationId()==null||!a.publicationId().equals(claim.publicationId())||!a.execution().checksum().equals(claim.publicationChecksum()))throw new IllegalArgumentException("ING_ACTIVE_PUBLICATION_CHANGED");return a.execution();}
 public void verifyForRetry(ExecutionBundle snapshot){
  var published=active(snapshot.sourceId());
  if(!published.enabled() || !published.publicationId().toString().equals(snapshot.configuration().get("publicationId"))
      || !published.execution().checksum().equals(snapshot.checksum()))throw new IllegalArgumentException("ING_ACTIVE_PUBLICATION_CHANGED");
  preflight(published.execution());
 }
 private PublishedActivation active(String source){return decode(getConfiguration("/api/onboarding/v1/runtime/publications/"+escape(source)+"/active"));}
 @Override public void preflight(Collection<String> refs){throw new IllegalArgumentException("ING_EXACT_SEMANTIC_BINDINGS_REQUIRED");}
 @Override public ExecutionBundle loadHistoricalAndVerify(String source,String reference){
  if(reference==null||reference.isBlank()||reference.length()>2048)throw new IllegalArgumentException("ING_HISTORICAL_REFERENCE_REQUIRED");
  var result=decode(getConfiguration("/api/onboarding/v1/runtime/publications/resolve?bundleRef="+escape(reference))).execution();
  if(!source.equals(result.sourceId())||!reference.equals(result.bundleId()+":"+result.bundleVersion()+":"+result.checksum()))throw new IllegalArgumentException("ING_HISTORICAL_REFERENCE_MISMATCH");
  verifyReferences(result,Resolution.HISTORICAL);return result;
 }
 @Override public void preflight(ExecutionBundle bundle){verifyReferences(bundle,Resolution.PUBLISHED);}
 @Override public void prepare(ExecutionBundle bundle){verifyReferences(bundle,Resolution.PREPARATION);}
 enum Resolution {
  HISTORICAL, PUBLISHED, PREPARATION;
  boolean accepts(Map<?,?> resolved) {
   Object status=resolved.get("status");
   Object kind=resolved.get("artifact_type");
   return "ACTIVE".equals(status) || ((this==HISTORICAL || this==PUBLISHED && ("DATATYPE".equals(kind) || "VALUE_MAP".equals(kind)))
       && ("DEPRECATED".equals(status) || "RETIRED".equals(status)));
  }
 }
 private void verifyReferences(ExecutionBundle bundle,Resolution mode){
  var published=PublishedActivation.map(bundle.configuration(),"publishedBundle");Object raw=published.get("semanticReferenceBindings");if(!(raw instanceof List<?> bindings)||bindings.isEmpty())throw new IllegalArgumentException("ING_EXACT_SEMANTIC_BINDINGS_REQUIRED");
  var pins=ValuePins.embedded(bindings);
  references.verify(bindings,(Collection<?>)bundle.configuration().get("pinnedReferences"),mode);
  if(published.get("semanticMapping") instanceof Map<?,?> mapping
      && (mapping.containsKey("compositions") || mapping.get("propertyMappings") instanceof List<?> properties
          && properties.stream().anyMatch(p -> p instanceof Map<?,?> field
              && (field.containsKey("transformParams") || "VALUE_MAP_V1".equals(field.get("transform"))))))
    new ConfiguredValueMapping(PublishedExecutionMapper.map(bundle).configuration(),pins);
 }
 Map<String,Object> resolveBatch(byte[] body){return exchange(ReferenceBatch.PATH,body,200);}
 Map<String,Object> resolveOne(ReferenceBatch.Pin pin){
  return get("/api/semantic/v1/references:resolve?semanticId="+escape(pin.id())+"&revisionId="+pin.revision()+"&publicationSetId="+pin.publication());
 }
 public Map<String,Object> frozenConfiguration(String source,UUID version,String hash){return getConfiguration("/api/internal/v1/onboarding/ingestion-configuration?sourceId="+escape(source)+"&onboardingVersionId="+version+"&configurationHash="+escape(hash));}
 public Map<String,Object> compatibility(String source,UUID version,String hash){return get("/api/internal/v1/onboarding/compatibility/ingestion-runtime?sourceId="+escape(source)+"&onboardingVersionId="+version+"&configurationHash="+escape(hash));}
 public Map<String,Object> attestCompatibility(String source,UUID version,String hash,long rows){return exchange("/api/internal/v1/onboarding/compatibility/ingestion-runtime",Map.of("sourceId",source,"onboardingVersionId",version.toString(),"configurationHash",hash,"compatible",true,"detail","Bounded frozen managed configuration probe PASS; validatedRows="+rows),201);}
 private Map<String,Object> get(String path){return exchange(path,null,200);}
 private Map<String,Object> getConfiguration(String path){return exchange(path,null,200,MAX_CONFIGURATION_RESPONSE_BYTES);}
 private Map<String,Object> exchange(String path,Object body,int expected){return exchange(path,body,expected,MAX_RESPONSE_BYTES);}
 @SuppressWarnings("unchecked") private Map<String,Object> exchange(String path,Object body,int expected,int maxResponseBytes){
  boolean batch=path.equals(ReferenceBatch.PATH);
  boolean semantic=batch || path.startsWith("/api/semantic/v1/references:resolve?");
  GatewayPacing.Lane lane=semantic?pacing.references:null;
  if(lane!=null)lane.acquireForRead();
  try{String token=WorkloadTokenFile.read(tokenFile);
   if(Thread.currentThread().isInterrupted())throw new InterruptedException();
   var builder=HttpRequest.newBuilder(gateway.resolve(path)).timeout(Duration.ofSeconds(4)).header("Authorization","Bearer "+token).header("Accept","application/json").header("X-Correlation-ID",UUID.randomUUID().toString());
   if(body==null)builder.GET();else builder.header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofByteArray(body instanceof byte[] encoded?encoded:json.writeValueAsBytes(body)));
   var pending=transport.send(builder.build(),info->info.statusCode()==expected?new BoundedBody(maxResponseBytes):batch && info.statusCode()==413?new BoundedBody(8192):HttpResponse.BodySubscribers.replacing(new byte[0]));HttpResponse<byte[]> response;
   try{response=pending.get(5,TimeUnit.SECONDS);}catch(TimeoutException timeout){pending.cancel(true);throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_TIMEOUT",AdapterSpi.ErrorClass.TRANSIENT_SOURCE,pacing.retryDelay(lane,GatewayPacing.NO_HEADERS));}catch(ExecutionException failed){pending.cancel(true);
     if(failed.getCause() instanceof IOException io && RESPONSE_TOO_LARGE.equals(io.getMessage()))
       throw new PublicationResponseTooLarge();
     String code=failed.getCause() instanceof HttpTimeoutException?"ING_ACTIVATION_TIMEOUT":"ING_ACTIVATION_RESPONSE_FAILED";
     throw new RuntimePorts.GatewayFailure(code,AdapterSpi.ErrorClass.TRANSIENT_SOURCE,pacing.retryDelay(lane,GatewayPacing.NO_HEADERS));}catch(InterruptedException cancelled){pending.cancel(true);throw cancelled;}
   int status=response.statusCode();
   boolean retry=Set.of(429,502,503,504).contains(status);
   if(lane!=null && !retry)lane.observeQuota(response.headers());
   if(batch && (status==404 || status==405))throw new ReferenceBatch.EndpointUnavailable();
   if(batch && status==413){
    try{
     var error=json.readTree(response.body());
     if(error!=null && "SEM_REFERENCE_BATCH_TOO_LARGE".equals(error.path("code").asText()))throw new ReferenceBatch.TooLarge();
    }catch(com.fasterxml.jackson.core.JsonProcessingException ignored){ /* Preserve the HTTP failure. */ }
   }
   if(status!=expected){
    var type=status==401 || status==403?AdapterSpi.ErrorClass.AUTH_ROUTE:retry?AdapterSpi.ErrorClass.TRANSIENT_SOURCE:AdapterSpi.ErrorClass.CONFIGURATION;
    Duration wait=retry?pacing.retryDelay(lane,response.headers()):null;
    throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_GATEWAY_"+status,type,wait);
   }
   if(batch){
    try{
     Map<String,Object> decoded=json.readerFor(Map.class)
         .with(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
         .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readValue(response.body());
     if(decoded==null)throw new ReferenceBatch.InvalidReply();
     return decoded;
    }catch(com.fasterxml.jackson.core.JsonProcessingException invalid){throw new ReferenceBatch.InvalidReply();}
   }
   var decoded=json.readValue(response.body(),Map.class);
   if(decoded==null)throw new IllegalArgumentException("ING_PUBLICATION_RESPONSE_INVALID");
   return decoded;
  }catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_INTERRUPTED",AdapterSpi.ErrorClass.CONFIGURATION);}catch(IOException e){throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_IO_FAILED",AdapterSpi.ErrorClass.CONFIGURATION);}
 }
 @FunctionalInterface
 interface Transport {
  CompletableFuture<HttpResponse<byte[]>> send(HttpRequest request,HttpResponse.BodyHandler<byte[]> handler);
 }
 private static final class PublicationResponseTooLarge extends IllegalArgumentException {
  PublicationResponseTooLarge(){super(RESPONSE_TOO_LARGE);}
 }
 private static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
  private final int maxBytes;
  private final CompletableFuture<byte[]> result=new CompletableFuture<>();private ByteArrayOutputStream bytes=new ByteArrayOutputStream();private Flow.Subscription subscription;
  BoundedBody(int maxBytes){this.maxBytes=maxBytes;}
  public CompletionStage<byte[]> getBody(){return result;}
  public void onSubscribe(Flow.Subscription s){subscription=s;s.request(1);}
  public void onNext(List<ByteBuffer> buffers){
   if(bytes==null)return;
   for(var buffer:buffers){
    if(buffer.remaining()>maxBytes-bytes.size()){
     bytes=null;
     result.completeExceptionally(new IOException(RESPONSE_TOO_LARGE));
     subscription.cancel();
     return;
    }
    byte[] chunk=new byte[buffer.remaining()];buffer.get(chunk);bytes.writeBytes(chunk);
   }
   subscription.request(1);
  }
  public void onError(Throwable error){bytes=null;result.completeExceptionally(error);}
  public void onComplete(){
   if(bytes==null)return;
   byte[] body=bytes.toByteArray();
   bytes=null;
   result.complete(body);
  }
 }
 private static String escape(String s){return URLEncoder.encode(s,java.nio.charset.StandardCharsets.UTF_8);}
 public record Page(List<Map<String,Object>> items,String nextAfter){}
}
