package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/** Authenticated Gateway-only execution transport with bounded complete exchanges. */
@Component @Primary
@ConditionalOnProperty(name="ouf.ingestion.execution.enabled",havingValue="true")
public class ExecutionGatewayClient implements RuntimePorts.ManagedObjectPort,RuntimePorts.GatewaySourcePort,RuntimePorts.DataLakePort,RuntimePorts.DurableHandoffPort {
  private final ObjectMapper json;private final RunExecutionRepository runs;private final URI gateway;private final Path token;private final HttpClient http;
  public ExecutionGatewayClient(ObjectMapper json,RunExecutionRepository runs,@Value("${ouf.ingestion.activation.gateway-url}") String url,@Value("${ouf.ingestion.activation.token-file}") String token){
    this.json=json;this.runs=runs;this.gateway=URI.create(url);this.token=Path.of(token);
    if(!Set.of("http","https").contains(gateway.getScheme())||gateway.getHost()==null||gateway.getUserInfo()!=null||gateway.getQuery()!=null||gateway.getFragment()!=null||!Set.of("","/").contains(gateway.getPath()))throw new IllegalArgumentException("ING_EXECUTION_GATEWAY_INVALID");
    http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).followRedirects(HttpClient.Redirect.NEVER).build();
  }
  public byte[] read(String ref,long size,String hash){
    if(size<1||size>50_000_000||!ref.matches("object://[a-z0-9._/-]+")||!hash.matches("sha256:[a-f0-9]{64}"))throw new IllegalArgumentException("ING_MANAGED_REFERENCE_INVALID");
    byte[] data=exchange("/internal/object-storage/v1/content?ref="+escape(ref),null,Math.toIntExact(size),200);
    if(data.length!=size||!hash.equals(hash(data)))throw new IllegalArgumentException("ING_MANAGED_INTEGRITY_MISMATCH");return data;
  }
  public byte[] fetch(String binding,Map<String,Object> parameters,String correlation){
    if(binding==null||!binding.startsWith("gateway://"))throw new IllegalArgumentException("ING_GATEWAY_BINDING_REQUIRED");
    return exchange("/internal/sources/v1/fetch",Map.of("bindingRef",binding,"parameters",parameters,"correlationId",correlation),50_000_000,200);
  }
  public RuntimePorts.DataLakePort.Receipt persist(RuntimePorts.DataLakePort.Zone zone,UUID run,String object,byte[] content,String hash,String key){
    Map<String,Object> body;
    try {
      var bundle=PublishedExecutionMapper.map(runs.bundle(run));
      if(content.length>50_000_000 || !hash.equals(hash(content)))
        throw failure("ING_LAKE_CONTENT_INVALID",AdapterSpi.ErrorClass.CONFIGURATION);
      body=Map.of("runId",run.toString(),"sourceId",bundle.sourceId(),"typeCode",PublishedActivation.text(bundle.configuration(),"typeCode"),
          "sourceObjectId",object,"zone",zone.name(),"contentBase64",Base64.getEncoder().encodeToString(content),"contentHash",hash,"idempotencyKey",key);
    } catch(RuntimePorts.GatewayFailure classified){throw classified;}
      catch(RuntimeException invalid){throw failure("ING_LAKE_REQUEST_CONFIG_INVALID",AdapterSpi.ErrorClass.CONFIGURATION);}
    byte[] reply=exchange(LAKE_PATH,body,65536,201);
    Map<?,?> response;
    try{
      response=json.readerFor(Map.class).with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readValue(reply);
    }catch(IOException malformed){throw failure("ING_EXECUTION_RECEIPT_INVALID",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);}
    if(response==null || !(response.get("durable") instanceof Boolean durable))
      throw failure("ING_EXECUTION_RECEIPT_INVALID",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
    if(response.get("contentHash") instanceof String receivedHash && !hash.equals(receivedHash))
      throw failure("ING_LAKE_CONTENT_DIVERGED",AdapterSpi.ErrorClass.CONFIGURATION);
    if(!durable)throw failure("ING_DATALAKE_NOT_DURABLE",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
    if(!hash.equals(response.get("contentHash")) || !(response.get("objectRef") instanceof String ref) || ref.isBlank())
      throw failure("ING_EXECUTION_RECEIPT_INVALID",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
    return new RuntimePorts.DataLakePort.Receipt(ref,true);
  }
  public RuntimePorts.DurableHandoffPort.Receipt deliver(UUID handoff,Map<String,Object> payload,String key){
    if(!handoff.toString().equals(payload.get("handoffId")))throw new IllegalArgumentException("ING_HANDOFF_ID_MISMATCH");
    var receipt=decode(exchange("/api/internal/v1/handoffs",payload,65536,201));
    String expected="udp://handoffs/"+handoff;
    if(!Boolean.TRUE.equals(receipt.get("durable"))||!expected.equals(receipt.get("receiptRef")))throw new IllegalArgumentException("ING_ACK_NOT_DURABLE");
    return new RuntimePorts.DurableHandoffPort.Receipt(expected,true);
  }
  private static final String LAKE_PATH="/api/internal/v1/lake/objects";
  private byte[] exchange(String path,Object body,int limit,int expected){
    boolean lake=LAKE_PATH.equals(path);
    HttpRequest request;
    try {
      var builder=HttpRequest.newBuilder(gateway.resolve(path)).timeout(Duration.ofSeconds(10))
          .header("Authorization","Bearer "+WorkloadTokenFile.read(token)).header("X-Correlation-ID",UUID.randomUUID().toString());
      if(body==null)builder.GET();
      else {
        byte[] encoded=json.writeValueAsBytes(body);
        if(encoded.length>10_485_760)throw failure("ING_EXECUTION_REQUEST_TOO_LARGE",AdapterSpi.ErrorClass.CONFIGURATION);
        builder.header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofByteArray(encoded));
      }
      request=builder.build();
    }catch(IOException invalid){throw failure("ING_EXECUTION_SERIALIZATION_FAILED",AdapterSpi.ErrorClass.CONFIGURATION);}
     catch(RuntimePorts.GatewayFailure classified){throw classified;}
     catch(RuntimeException invalid){throw failure("ING_EXECUTION_REQUEST_INVALID",AdapterSpi.ErrorClass.CONFIGURATION);}
    CompletableFuture<HttpResponse<byte[]>> pending;
    try{
      pending=http.sendAsync(request,info->info.statusCode()==expected || (lake && info.statusCode()>=500)
          ?new BoundedBody(limit):HttpResponse.BodySubscribers.replacing(new byte[0]));
    }catch(IllegalArgumentException invalid){throw failure("ING_EXECUTION_REQUEST_INVALID",AdapterSpi.ErrorClass.CONFIGURATION);}
    HttpResponse<byte[]> response;
    try{response=pending.get(12,TimeUnit.SECONDS);}
    catch(InterruptedException interrupted){
      pending.cancel(true);Thread.currentThread().interrupt();
      throw failure("ING_EXECUTION_INTERRUPTED",AdapterSpi.ErrorClass.CONFIGURATION);
    }catch(TimeoutException|ExecutionException e){
      pending.cancel(true);
      for(Throwable cause=e;cause!=null;cause=cause.getCause()){
        if(cause instanceof HttpConnectTimeoutException || cause instanceof ConnectException || cause instanceof UnknownHostException)
          throw failure("ING_EXECUTION_CONNECT_FAILED",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
        if(cause instanceof HttpTimeoutException || cause instanceof TimeoutException)
          throw failure(lake?"ING_LAKE_RESPONSE_TIMEOUT":"ING_EXECUTION_GATEWAY_UNAVAILABLE",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
        if(cause instanceof IOException && "ING_RESPONSE_LIMIT".equals(cause.getMessage()))
          throw failure("ING_RESPONSE_LIMIT",lake?AdapterSpi.ErrorClass.TRANSIENT_SOURCE:AdapterSpi.ErrorClass.CONFIGURATION);
        if(cause instanceof javax.net.ssl.SSLHandshakeException)
          throw failure("ING_EXECUTION_TLS_FAILED",AdapterSpi.ErrorClass.CONFIGURATION);
      }
      if(lake && !(e.getCause() instanceof IOException))
        throw failure("ING_LAKE_OUTCOME_UNKNOWN",AdapterSpi.ErrorClass.CONFIGURATION);
      throw failure(lake?"ING_LAKE_TRANSPORT_UNCONFIRMED":"ING_EXECUTION_GATEWAY_UNAVAILABLE",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
    }
    int status=response.statusCode();
    if(status!=expected){
      if(lake && status>=500 && duplicateNotUsable(response.body()))
        throw failure("UDP_LAKE_DUPLICATE_NOT_USABLE",AdapterSpi.ErrorClass.CONFIGURATION);
      boolean retry=(lake && status==408) || status==429 || status==502 || status==503 || status==504 ||
        (status==202 && "/api/internal/v1/handoffs".equals(path));
      var type=retry?AdapterSpi.ErrorClass.TRANSIENT_SOURCE:status==401||status==403?AdapterSpi.ErrorClass.AUTH_ROUTE:AdapterSpi.ErrorClass.CONFIGURATION;
      throw new RuntimePorts.GatewayFailure("ING_EXECUTION_GATEWAY_"+status,type,retry?RetryPolicy.retryAfter(response.headers()):null);
    }
    return response.body();
  }

  private boolean duplicateNotUsable(byte[] body){
    try {
      var error=json.readTree(body);
      if(error!=null)for(String field:List.of("code","errorCode","message","detail"))
        if("UDP_LAKE_DUPLICATE_NOT_USABLE".equals(error.path(field).asText()))return true;
    }catch(IOException malformed){/* Keep the classified HTTP status. */}
    return false;
  }

  @SuppressWarnings("unchecked") private Map<String,Object> decode(byte[] value){try{return json.readValue(value,Map.class);}catch(IOException e){throw new IllegalArgumentException("ING_EXECUTION_RECEIPT_INVALID");}}
  static String hash(byte[] content){try{return "sha256:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));}catch(Exception e){throw new IllegalStateException("ING_HASH_FAILED",e);}}
  private static String escape(String text){return URLEncoder.encode(text,StandardCharsets.UTF_8);}
  private static RuntimePorts.GatewayFailure failure(String code,AdapterSpi.ErrorClass type){return new RuntimePorts.GatewayFailure(code,type);}
  private static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]>{
    private final int limit;private final CompletableFuture<byte[]> result=new CompletableFuture<>();private final ByteArrayOutputStream bytes=new ByteArrayOutputStream();private Flow.Subscription subscription;
    BoundedBody(int limit){this.limit=limit;}public CompletionStage<byte[]> getBody(){return result;}
    public void onSubscribe(Flow.Subscription s){subscription=s;s.request(1);}
    public void onNext(List<ByteBuffer> buffers){for(var b:buffers){if((long)bytes.size()+b.remaining()>limit){subscription.cancel();result.completeExceptionally(new IOException("ING_RESPONSE_LIMIT"));return;}byte[] chunk=new byte[b.remaining()];b.get(chunk);bytes.writeBytes(chunk);}subscription.request(1);}
    public void onError(Throwable e){result.completeExceptionally(e);}public void onComplete(){result.complete(bytes.toByteArray());}
  }
}
