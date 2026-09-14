package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/**
 * Concrete governed Gateway client. It calls only the configured OUF Gateway endpoint;
 * source URLs and credentials never cross the adapter boundary.
 */
@Service
@ConditionalOnProperty(name="ouf.ingestion.gateway.base-url")
public final class HttpGatewaySourceClient implements RuntimePorts.GatewaySourcePort {
  private final HttpClient http;private final URI endpoint;private final ObjectMapper json;private final Duration requestTimeout;private final int maxResponseBytes;private final Clock clock;

  public HttpGatewaySourceClient(ObjectMapper json,
      @Value("${ouf.ingestion.gateway.base-url}") String endpoint,
      @Value("${ouf.ingestion.gateway.connect-timeout-ms:5000}") long connectTimeoutMs,
      @Value("${ouf.ingestion.gateway.request-timeout-ms:30000}") long requestTimeoutMs,
      @Value("${ouf.ingestion.gateway.max-response-bytes:50000000}") int maxResponseBytes){
    this(HttpClient.newBuilder().connectTimeout(positive(connectTimeoutMs,"ING_GATEWAY_CONNECT_TIMEOUT_INVALID")).followRedirects(HttpClient.Redirect.NEVER).build(),uri(endpoint),json,positive(requestTimeoutMs,"ING_GATEWAY_REQUEST_TIMEOUT_INVALID"),maxResponseBytes,Clock.systemUTC());
  }

  HttpGatewaySourceClient(HttpClient http,URI endpoint,ObjectMapper json,Duration requestTimeout,int maxResponseBytes,Clock clock){
    this.http=Objects.requireNonNull(http);this.endpoint=validate(endpoint);this.json=Objects.requireNonNull(json);this.requestTimeout=Objects.requireNonNull(requestTimeout);this.clock=Objects.requireNonNull(clock);
    if(requestTimeout.isZero()||requestTimeout.isNegative())throw new IllegalArgumentException("ING_GATEWAY_REQUEST_TIMEOUT_INVALID");
    if(maxResponseBytes<1)throw new IllegalArgumentException("ING_GATEWAY_MAX_RESPONSE_INVALID");this.maxResponseBytes=maxResponseBytes;
  }

  @Override public byte[] fetch(String governedBindingRef,Map<String,Object> request,String correlationId){
    if(governedBindingRef==null||!governedBindingRef.startsWith("gateway://"))throw failure("ING_GATEWAY_BINDING_REQUIRED",AdapterSpi.ErrorClass.CONFIGURATION,null);
    if(request==null)throw failure("ING_GATEWAY_REQUEST_INVALID",AdapterSpi.ErrorClass.CONFIGURATION,null);
    if(correlationId==null||!correlationId.matches("[A-Za-z0-9._:-]{1,128}"))throw failure("ING_GATEWAY_CORRELATION_INVALID",AdapterSpi.ErrorClass.CONFIGURATION,null);
    byte[] body;try{body=json.writeValueAsBytes(Map.of("bindingRef",governedBindingRef,"parameters",request,"correlationId",correlationId));}catch(Exception e){throw failure("ING_GATEWAY_REQUEST_INVALID",AdapterSpi.ErrorClass.CONFIGURATION,null);}
    HttpRequest outbound=HttpRequest.newBuilder(endpoint).timeout(requestTimeout).header("Content-Type","application/json").header("Accept","application/octet-stream, application/json, application/xml").header("X-Correlation-ID",correlationId).POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
    try{
      HttpResponse<InputStream> response=http.send(outbound,HttpResponse.BodyHandlers.ofInputStream());int status=response.statusCode();
      try(InputStream input=response.body()){
        byte[] bytes=input.readNBytes(maxResponseBytes+1);if(bytes.length>maxResponseBytes)throw failure("ING_GATEWAY_RESPONSE_TOO_LARGE",AdapterSpi.ErrorClass.DATA,null);
        if(status>=200&&status<300)return bytes;
      }
      throw classified(status,response.headers());
    }catch(RuntimePorts.GatewayFailure e){throw e;}catch(HttpTimeoutException|ConnectException e){throw failure("ING_GATEWAY_UNAVAILABLE",AdapterSpi.ErrorClass.TRANSIENT_SOURCE,null);}catch(InterruptedException e){Thread.currentThread().interrupt();throw failure("ING_GATEWAY_INTERRUPTED",AdapterSpi.ErrorClass.TRANSIENT_SOURCE,null);}catch(IOException e){throw failure("ING_GATEWAY_IO_FAILURE",AdapterSpi.ErrorClass.TRANSIENT_SOURCE,null);}
  }

  private RuntimePorts.GatewayFailure classified(int status,HttpHeaders headers){
    if(status==429)return failure("ING_GATEWAY_429",AdapterSpi.ErrorClass.TRANSIENT_SOURCE,retryAfter(headers).orElse(null));
    if(status==408||status>=500)return failure("ING_GATEWAY_"+status,AdapterSpi.ErrorClass.TRANSIENT_SOURCE,null);
    if(status==401||status==403)return failure("ING_GATEWAY_"+status,AdapterSpi.ErrorClass.AUTH_ROUTE,null);
    return failure("ING_GATEWAY_"+status,AdapterSpi.ErrorClass.CONFIGURATION,null);
  }

  private Optional<Duration> retryAfter(HttpHeaders headers){
    Optional<String> value=headers.firstValue("Retry-After");if(value.isEmpty()||value.orElseThrow().isBlank())return Optional.empty();String raw=value.orElseThrow().strip();
    try{long seconds=Long.parseLong(raw);return seconds>0?Optional.of(Duration.ofSeconds(seconds)):Optional.empty();}catch(NumberFormatException ignored){}
    try{Instant at=ZonedDateTime.parse(raw,DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();long seconds=ChronoUnit.SECONDS.between(clock.instant(),at);return seconds>0?Optional.of(Duration.ofSeconds(seconds)):Optional.empty();}catch(Exception ignored){return Optional.empty();}
  }

  private static RuntimePorts.GatewayFailure failure(String code,AdapterSpi.ErrorClass type,Duration retry){return new RuntimePorts.GatewayFailure(code,type,retry);}
  private static Duration positive(long millis,String code){if(millis<1)throw new IllegalArgumentException(code);return Duration.ofMillis(millis);}
  private static URI uri(String value){try{return URI.create(value);}catch(Exception e){throw new IllegalArgumentException("ING_GATEWAY_ENDPOINT_INVALID",e);}}
  private static URI validate(URI value){if(value==null||value.getUserInfo()!=null||value.getFragment()!=null||!Set.of("http","https").contains(value.getScheme())||value.getHost()==null)throw new IllegalArgumentException("ING_GATEWAY_ENDPOINT_INVALID");return value;}
}
