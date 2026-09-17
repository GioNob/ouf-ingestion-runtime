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
    var bundle=PublishedExecutionMapper.map(runs.bundle(run));
    if(content.length>50_000_000||!hash.equals(hash(content)))throw new IllegalArgumentException("ING_LAKE_CONTENT_INVALID");
    var response=decode(exchange("/api/internal/v1/lake/objects",Map.of("runId",run.toString(),"sourceId",bundle.sourceId(),"typeCode",PublishedActivation.text(bundle.configuration(),"typeCode"),"sourceObjectId",object,"zone",zone.name(),"contentBase64",Base64.getEncoder().encodeToString(content),"contentHash",hash,"idempotencyKey",key),65536,201));
    if(!Boolean.TRUE.equals(response.get("durable"))||!hash.equals(response.get("contentHash")))throw new IllegalArgumentException("ING_DATALAKE_NOT_DURABLE");
    return new RuntimePorts.DataLakePort.Receipt(PublishedActivation.text(response,"objectRef"),true);
  }
  public RuntimePorts.DurableHandoffPort.Receipt deliver(UUID handoff,Map<String,Object> payload,String key){
    if(!handoff.toString().equals(payload.get("handoffId")))throw new IllegalArgumentException("ING_HANDOFF_ID_MISMATCH");
    var receipt=decode(exchange("/api/internal/v1/handoffs",payload,65536,201));
    String expected="udp://handoffs/"+handoff;
    if(!Boolean.TRUE.equals(receipt.get("durable"))||!expected.equals(receipt.get("receiptRef")))throw new IllegalArgumentException("ING_ACK_NOT_DURABLE");
    return new RuntimePorts.DurableHandoffPort.Receipt(expected,true);
  }
  private byte[] exchange(String path,Object body,int limit,int expected){
    try{
      String credential=Files.readString(token).strip();if(credential.isEmpty()||credential.length()>16384||credential.chars().anyMatch(Character::isWhitespace))throw new IllegalArgumentException("ING_WORKLOAD_TOKEN_INVALID");
      var builder=HttpRequest.newBuilder(gateway.resolve(path)).timeout(Duration.ofSeconds(10)).header("Authorization","Bearer "+credential).header("X-Correlation-ID",UUID.randomUUID().toString());
      if(body==null)builder.GET();else {byte[] encoded=json.writeValueAsBytes(body);if(encoded.length>10_485_760)throw new IllegalArgumentException("ING_EXECUTION_REQUEST_TOO_LARGE");builder.header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofByteArray(encoded));}
      var pending=http.sendAsync(builder.build(),info->new BoundedBody(limit));HttpResponse<byte[]> response;
      try{response=pending.get(12,TimeUnit.SECONDS);}catch(TimeoutException|ExecutionException e){pending.cancel(true);throw failure("ING_EXECUTION_GATEWAY_UNAVAILABLE",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);}
      if(response.statusCode()!=expected)throw failure("ING_EXECUTION_GATEWAY_"+response.statusCode(),response.statusCode()==401||response.statusCode()==403?AdapterSpi.ErrorClass.AUTH_ROUTE:response.statusCode()>=500||response.statusCode()==429?AdapterSpi.ErrorClass.TRANSIENT_SOURCE:AdapterSpi.ErrorClass.CONFIGURATION);
      return response.body();
    }catch(InterruptedException e){Thread.currentThread().interrupt();throw failure("ING_EXECUTION_INTERRUPTED",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);}catch(IOException e){throw failure("ING_EXECUTION_IO_FAILED",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);}
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
