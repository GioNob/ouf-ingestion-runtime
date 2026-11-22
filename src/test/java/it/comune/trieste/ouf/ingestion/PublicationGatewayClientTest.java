package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class PublicationGatewayClientTest {
 private static final int TWO_MIB=2*1024*1024, THREE_MIB=3*1024*1024;
 private static final String PUBLICATIONS="/api/onboarding/v1/runtime/publications";

 @Test void frozenConfigurationAcceptsThreeMebibytesAndCompatibilityRemainsAtTwo() throws Exception {
  var size=new AtomicInteger(THREE_MIB);
  var token=Files.createTempFile("frozen-response-test-",".token");
  var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  try {
   Files.writeString(token,"test-token");
   server.createContext("/",exchange->{
    try {
     byte[] body=pad(utf8("{}"),size.get());
     exchange.sendResponseHeaders(200,0);
     exchange.getResponseBody().write(body);
    } finally {exchange.close();}
   });
   server.start();
   var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");
   var version=UUID.randomUUID();
   assertThat(client.frozenConfiguration("test-source",version,"test-hash")).isEmpty();
   size.set(THREE_MIB+1);
   assertThatThrownBy(()->client.frozenConfiguration("test-source",version,"test-hash"))
       .hasMessage("ING_PUBLICATION_RESPONSE_TOO_LARGE");
   size.set(TWO_MIB);
   assertThat(client.compatibility("test-source",version,"test-hash")).isEmpty();
   size.set(TWO_MIB+1);
   assertThatThrownBy(()->client.compatibility("test-source",version,"test-hash"))
       .hasMessage("ING_PUBLICATION_RESPONSE_TOO_LARGE");
  } finally {server.stop(0);Files.deleteIfExists(token);}
 }

 @Test void discoverPaginatesAllLargePublicationsInOrder() throws Exception {
  var sourceIds=new ArrayList<String>();
  var rows=new ArrayList<byte[]>();
  var pins=List.of(largeValueMapPin(1,354),largeValueMapPin(2,356));
  ValuePins.embedded(pins);
  for(int i=0;i<20;i++){
   String sourceId="source-"+(10+i);
   sourceIds.add(sourceId);
   var envelope=PublishedActivationTest.envelope(sourceId,true,1);
   var bundle=PublishedActivation.map(envelope,"bundle");
   bundle.put("semanticReferenceBindings",pins);
   bundle.put("semanticRefs",pins.stream().map(pin->pin.get("semanticId")+"@1").toList());
   PublishedActivationTest.hash(bundle);
   envelope.put("checksum",bundle.get("checksum"));
   rows.add(PublishedActivationTest.JSON.writeValueAsBytes(envelope));
  }
  assertThat(rows.getFirst().length+rows.get(1).length).isGreaterThan(TWO_MIB);

  var requests=new ArrayList<String>();
  var token=Files.createTempFile("discover-large-test-",".token");
  try {
   var client=inMemoryClient(token,request->{
    assertThat(request.uri().getPath()).isEqualTo(PUBLICATIONS);
    String query=request.uri().getQuery();
    requests.add(query);
    String[] parts=query.split("&after=",2);
    int limit=Integer.parseInt(parts[0].substring("limit=".length()));
    int start=sourceIds.indexOf(parts[1])+1;
    int end=Math.min(start+limit,rows.size());
    return pageReply(rows.subList(start,end),end-start==limit?sourceIds.get(end-1):"");
   });
   var discovered=new ArrayList<String>();
   String cursor="";
   for(int offset=0;offset<sourceIds.size();offset+=2){
    var page=client.discover(cursor);
    assertThat(page.items()).hasSize(2);
    boolean last=offset+2==sourceIds.size();
    assertThat(requests).containsExactlyElementsOf(last?pageQueries(cursor,20):pageQueries(cursor,20,10,5,2));
    requests.clear();
    discovered.addAll(page.items().stream().map(client::decode).map(PublishedActivation::sourceId).toList());
    assertThat(page.nextAfter()).isEqualTo(last?"":sourceIds.get(offset+1));
    cursor=page.nextAfter();
   }
   var terminal=client.discover(sourceIds.getLast());
   assertThat(terminal.items()).isEmpty();
   assertThat(terminal.nextAfter()).isEmpty();
   assertThat(requests).containsExactly("limit=20&after="+sourceIds.getLast());
   assertThat(discovered).containsExactlyElementsOf(sourceIds);
  } finally {Files.deleteIfExists(token);}
 }

 @Test void activeAndResolveEnforceTheThreeMebibyteBoundary() throws Exception {
  String source="test-source";
  var envelope=widePublication(source,THREE_MIB);
  var bundle=PublishedActivation.decode(envelope,"tenant-a",PublishedActivationTest.JSON).execution();
  String reference=bundle.bundleId()+":"+bundle.bundleVersion()+":"+bundle.checksum();
  byte[] exact=PublishedActivationTest.JSON.writeValueAsBytes(envelope);
  assertThat(exact.length).isEqualTo(THREE_MIB);
  var oversized=new AtomicBoolean();
  var requests=new ArrayList<String>();
  var token=Files.createTempFile("publication-size-test-",".token");
  try {
   var client=inMemoryClient(token,request->{
    String path=request.uri().getPath();
    requests.add(path);
    if(path.equals("/api/semantic/v1/references:resolve")){
     return new Reply(200,List.of(utf8("""
         {"semantic_id":"core","semantic_version":"1","revision_id":"00000000-0000-0000-0000-000000000001",
          "publication_set_id":"00000000-0000-0000-0000-000000000002","status":"ACTIVE"}
         """)));
    }
    if(path.equals(PUBLICATIONS+"/resolve"))assertThat(request.uri().getQuery()).isEqualTo("bundleRef="+reference);
    return new Reply(200,oversized.get()?List.of(exact,utf8(" ")):List.of(exact));
   });
   assertThat(client.loadAndVerify(source).checksum()).isEqualTo(bundle.checksum());
   assertThat(client.loadHistoricalAndVerify(source,reference).checksum()).isEqualTo(bundle.checksum());
   oversized.set(true);
   assertThatThrownBy(()->client.loadAndVerify(source)).hasMessage("ING_PUBLICATION_RESPONSE_TOO_LARGE");
   assertThatThrownBy(()->client.loadHistoricalAndVerify(source,reference)).hasMessage("ING_PUBLICATION_RESPONSE_TOO_LARGE");
   assertThat(requests).containsExactly(PUBLICATIONS+"/test-source/active",PUBLICATIONS+"/resolve",
       "/api/semantic/v1/references:resolve",PUBLICATIONS+"/test-source/active",PUBLICATIONS+"/resolve");
  } finally {Files.deleteIfExists(token);}
 }

 @Test void discoverRejectsOversizedSinglePublicationAfterFiveReads() throws Exception {
  var envelope=widePublication("source-02",THREE_MIB+1);
  byte[] row=PublishedActivationTest.JSON.writeValueAsBytes(envelope);
  assertThat(row.length).isEqualTo(THREE_MIB+1);
  var requests=new ArrayList<String>();
  var token=Files.createTempFile("discover-single-test-",".token");
  try {
   var client=inMemoryClient(token,request->{
    String query=request.uri().getQuery();
    requests.add(query);
    return pageReply(List.of(row),query.startsWith("limit=1&")?"source-02":"");
   });
   assertThatThrownBy(()->client.discover("source-01")).hasMessage("ING_PUBLICATION_RESPONSE_TOO_LARGE");
   assertThat(requests).containsExactlyElementsOf(pageQueries("source-01",20,10,5,2,1));
  } finally {Files.deleteIfExists(token);}
 }

 @Test void discoverValidatesTheReducedLimitAndCursor() throws Exception {
  var rows=List.of(utf8("{}"),utf8("{}"),utf8("{}"));
  var oversized=new Reply(200,List.of(pad(utf8("{}"),THREE_MIB+1)));
  var token=Files.createTempFile("discover-invalid-test-",".token");
  try {
   for(var invalid:List.of(pageReply(rows,"source-02"),pageReply(rows.subList(0,2),"source-01"))){
    var requests=new ArrayList<String>();
    var client=inMemoryClient(token,request->{
     String query=request.uri().getQuery();
     requests.add(query);
     return query.startsWith("limit=2&")?invalid:oversized;
    });
    assertThatThrownBy(()->client.discover("source-01")).hasMessage("ING_PUBLICATION_PAGE_INVALID");
    assertThat(requests).containsExactlyElementsOf(pageQueries("source-01",20,10,5,2));
   }
  } finally {Files.deleteIfExists(token);}
 }

 @Test void discoverDoesNotRepeatServiceUnavailable() throws Exception {
  var requests=new ArrayList<String>();
  var token=Files.createTempFile("discover-unavailable-test-",".token");
  try {
   var client=inMemoryClient(token,request->{
    requests.add(request.uri().getQuery());
    return new Reply(503,List.of());
   });
   assertThatThrownBy(()->client.discover("source-01"))
       .isInstanceOf(RuntimePorts.GatewayFailure.class).hasMessage("ING_ACTIVATION_GATEWAY_503");
   assertThat(requests).containsExactly("limit=20&after=source-01");
  } finally {Files.deleteIfExists(token);}
 }

 @Test void resolvedNestedDatatypeMustMatchDigestAndPreparationStatus() throws Exception {
  String parent="urn:type:parent", child="urn:type:child", set=UUID.randomUUID().toString();
  var definitions=Map.of(parent,Map.of("components",List.of(Map.of("name","nested","type",child+"@"+set,"cardinality","MANY","required",true))),
      child,Map.of("components",List.of(Map.of("name","name","type","string","cardinality","ONE","required",false))));
  var bindings=new ArrayList<Map<String,Object>>();
  for(String id:List.of(parent,child)) bindings.add(Map.of("semanticId",id,"semanticVersion","1","publicationSetId",set,
      "revisionId",UUID.randomUUID().toString(),"definition",definitions.get(id),"definitionSha256",ValuePins.digest(definitions.get(id))));
  var refs=List.of(parent+"@1",child+"@1");
  var bundle=new ExecutionBundle("test","1","test-hash","test-source","MANAGED",null,
      Map.of("pinnedReferences",refs,"publishedBundle",Map.of("semanticReferenceBindings",bindings)));
  var status=new AtomicReference<>("ACTIVE");var corrupt=new AtomicBoolean();
  var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  var token=Files.createTempFile("datatype-test-",".token");
  try {
   Files.writeString(token,"test-token");
   server.createContext("/api/semantic/v1/references:resolve",exchange->{
    try {
     var pin=exchange.getRequestURI().getQuery().contains("semanticId="+parent+"&")?bindings.getFirst():bindings.getLast();
     boolean nested=child.equals(pin.get("semanticId"));
     var resolved=Map.of("artifact_type","DATATYPE","semantic_id",pin.get("semanticId"),"semantic_version","1",
         "publication_set_id",set,"revision_id",pin.get("revisionId"),"status",nested?status.get():"ACTIVE",
         "definition",pin.get("definition"),"definitionSha256",nested && corrupt.get()?"0".repeat(64):pin.get("definitionSha256"));
     byte[] body=PublishedActivationTest.JSON.writeValueAsBytes(resolved);
     exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);
    } finally { exchange.close(); }
   });
   server.start();
   var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");
   client.prepare(bundle);
   for(String old:List.of("DEPRECATED","RETIRED")){
    status.set(old);client.preflight(bundle);
    assertThatThrownBy(()->client.prepare(bundle)).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");
   }
   status.set("ACTIVE");corrupt.set(true);
   assertThatThrownBy(()->client.preflight(bundle)).hasMessage("ING_VALUE_DEFINITION_INVALID");
  } finally {server.stop(0);Files.deleteIfExists(token);}
 }
 @Test void publishedValueMapMustMatchExactRegistryEvidenceInEveryResolutionMode() throws Exception {
  String id="urn:map", set=UUID.randomUUID().toString(), revision=UUID.randomUUID().toString();
  String target="urn:vocab@"+UUID.randomUUID();
  var definition=Map.of("targetVocabulary",target,"entries",List.of(Map.of("code","A","concept","urn:concept:a")));
  String digest=ValuePins.digest(definition);
  var pin=Map.of("semanticId",id,"semanticVersion","1","publicationSetId",set,"revisionId",revision,
      "valueMapRef",id+"@"+set,"targetVocabulary",target,"definition",definition,"definitionSha256",digest);
  var envelope=PublishedActivationTest.envelope("map-source",true,1);
  var published=PublishedActivation.map(envelope,"bundle");
  published.put("semanticRefs",List.of(id+"@1"));
  published.put("semanticReferenceBindings",List.of(pin));
  PublishedActivationTest.hash(published);
  envelope.put("checksum",published.get("checksum"));
  var exact=Map.<String,Object>of("artifact_type","VALUE_MAP","semantic_id",id,"semantic_version","1","publication_set_id",set,
      "revision_id",revision,"status","ACTIVE","definition",definition,"definitionSha256",digest);
  var actual=new AtomicReference<Map<String,Object>>(exact);
  var calls=new AtomicInteger();
  var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  var token=Files.createTempFile("value-map-test-",".token");
  try {
   Files.writeString(token,"test-token");
   server.createContext("/api/semantic/v1/references:resolve",exchange->{
    try {
     String query="semanticId="+id+"&revisionId="+revision+"&publicationSetId="+set;
     if(!query.equals(exchange.getRequestURI().getQuery()) ||
         !"Bearer test-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))){
      exchange.sendResponseHeaders(403,-1);return;
     }
     calls.incrementAndGet();
     byte[] body=PublishedActivationTest.JSON.writeValueAsBytes(actual.get());
     exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);
    } finally {exchange.close();}
   });
   server.createContext("/api/onboarding/v1/runtime/publications/resolve",exchange->{
    try {
     byte[] body=PublishedActivationTest.JSON.writeValueAsBytes(envelope);
     exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);
    } finally {exchange.close();}
   });
   server.start();
   var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");
   var bundle=client.decode(envelope).execution();
   String ref=bundle.bundleId()+":"+bundle.bundleVersion()+":"+bundle.checksum();
   client.prepare(bundle);
   for(String status:List.of("DEPRECATED","RETIRED")){
    var resolved=new HashMap<>(exact);resolved.put("status",status);actual.set(resolved);
    client.preflight(bundle);
    assertThat(client.loadHistoricalAndVerify("map-source",ref).checksum()).isEqualTo(bundle.checksum());
    assertThatThrownBy(()->client.prepare(bundle)).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");
   }
   for(var change:Map.<String,Object>of("definitionSha256","0".repeat(64),"definition",
       Map.of("targetVocabulary",target,"entries",List.of(Map.of("code","A","concept","urn:concept:other"))),
       "artifact_type","DATATYPE").entrySet()){
    var resolved=new HashMap<>(exact);resolved.put(change.getKey(),change.getValue());actual.set(resolved);
    assertThatThrownBy(()->client.preflight(bundle)).hasMessage("ING_VALUE_DEFINITION_INVALID").hasNoCause();
   }
   var differentRevision=new HashMap<>(exact);differentRevision.put("revision_id",UUID.randomUUID().toString());actual.set(differentRevision);
   assertThatThrownBy(()->client.preflight(bundle)).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");
   var header=new HashMap<>(pin);header.remove("definition");header.remove("definitionSha256");
   published.put("semanticReferenceBindings",List.of(header));
   PublishedActivationTest.hash(published);
   envelope.put("checksum",published.get("checksum"));
   var incomplete=client.decode(envelope).execution();
   int count=calls.get();
   assertThatThrownBy(()->client.preflight(incomplete)).hasMessage("ING_VALUE_MAP_CONTENT_REQUIRED");
   assertThat(calls).hasValue(count);
  } finally {server.stop(0);Files.deleteIfExists(token);}
 }
 @Test void nativeClassAndTwentyPropertiesRemainExactlyPinnedWithinTheBound()throws Exception{
  for(int count:List.of(21,32,33)){
   var envelope=PublishedActivationTest.envelope("native-source",true,1);var published=PublishedActivation.map(envelope,"bundle");
   var bindings=new ArrayList<Map<String,Object>>();var refs=new ArrayList<String>();
   for(int i=0;i<count;i++){String id="urn:native:"+i;refs.add(id+"@1");bindings.add(Map.of("semanticId",id,"semanticVersion","1","revisionId",new UUID(0,i+1).toString(),"publicationSetId",new UUID(1,1).toString()));}
   published.put("semanticReferenceBindings",bindings);published.put("semanticRefs",refs);PublishedActivationTest.hash(published);envelope.put("checksum",published.get("checksum"));
   var calls=new AtomicInteger();var corruptLast=new AtomicBoolean();Path token=Files.createTempFile("native-client-",".token");Files.writeString(token,"test-token");var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
   server.createContext("/",exchange->{try{if(!"Bearer test-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))){exchange.sendResponseHeaders(403,-1);return;}calls.incrementAndGet();var query=new HashMap<String,String>();for(String part:exchange.getRequestURI().getRawQuery().split("&")){String[] pair=part.split("=",2);query.put(pair[0],URLDecoder.decode(pair[1],java.nio.charset.StandardCharsets.UTF_8));}String id=query.get("semanticId");int index=Integer.parseInt(id.substring(id.lastIndexOf(':')+1));var response=Map.of("semantic_id",id,"semantic_version",corruptLast.get()&&index==count-1?"2":"1","revision_id",query.get("revisionId"),"publication_set_id",query.get("publicationSetId"),"status","ACTIVE");byte[] body=PublishedActivationTest.JSON.writeValueAsBytes(response);exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);}finally{exchange.close();}});server.start();
   try{var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");var bundle=client.decode(envelope).execution();
    if(count>32){assertThatThrownBy(()->client.preflight(bundle)).hasMessage("ING_EXACT_SEMANTIC_BINDINGS_REQUIRED");assertThat(calls).hasValue(0);}
    else{client.preflight(bundle);assertThat(calls).hasValue(count);corruptLast.set(true);assertThatThrownBy(()->client.preflight(bundle)).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");assertThat(calls).hasValue(count*2);}
   }finally{server.stop(0);Files.deleteIfExists(token);}
  }
 }
 @Test void exactReferenceAndActivePublicationAreRecheckedWithWorkloadCredential()throws Exception{
  var envelope=PublishedActivationTest.envelope("test-source",false,1);var mode=new AtomicInteger();var query=new AtomicReference<String>();Path token=Files.createTempFile("activation-client-",".token");Files.writeString(token,"test-token");HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  server.createContext("/",exchange->{try{if(!"Bearer test-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))||mode.get()==3){exchange.sendResponseHeaders(403,-1);return;}
   Map<String,Object> response;if(exchange.getRequestURI().getPath().startsWith("/api/semantic")){query.set(exchange.getRequestURI().getQuery());response=Map.of("semantic_id","core","semantic_version",mode.get()==1?"2":"1","revision_id","00000000-0000-0000-0000-000000000001","publication_set_id","00000000-0000-0000-0000-000000000002","status","ACTIVE");}else{response=new LinkedHashMap<>(envelope);if(mode.get()==2)response.put("publicationId",UUID.randomUUID().toString());}
   byte[] body=PublishedActivationTest.JSON.writeValueAsBytes(response);exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);
  }finally{exchange.close();}});server.start();
  try{var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");var a=client.decode(envelope);var claim=new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-a","test-source",60,"worker",a.publicationId(),a.execution().checksum(),java.time.OffsetDateTime.now(),1);
   assertThat(client.loadAndVerify(claim).checksum()).isEqualTo(a.execution().checksum());client.preflight(a.execution());assertThat(query.get()).contains("revisionId=00000000-0000-0000-0000-000000000001","publicationSetId=00000000-0000-0000-0000-000000000002");
   mode.set(1);assertThatThrownBy(()->client.preflight(a.execution())).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");mode.set(2);assertThatThrownBy(()->client.loadAndVerify(claim)).hasMessage("ING_ACTIVE_PUBLICATION_CHANGED");mode.set(3);assertThatThrownBy(()->client.loadAndVerify(claim)).isInstanceOf(RuntimePorts.GatewayFailure.class).hasMessage("ING_ACTIVATION_GATEWAY_403");
  }finally{server.stop(0);Files.deleteIfExists(token);}
 }

 private static Map<String,Object> widePublication(String source,int size) throws Exception {
  var envelope=PublishedActivationTest.envelope(source,true,1);
  var bundle=PublishedActivation.map(envelope,"bundle");
  var extraction=new LinkedHashMap<>(PublishedActivation.map(bundle,"extractionProfile"));
  bundle.put("extractionProfile",extraction);
  extraction.put("description","");
  int baseSize=PublishedActivationTest.JSON.writeValueAsBytes(envelope).length;
  extraction.put("description","x".repeat(size-baseSize));
  PublishedActivationTest.hash(bundle);
  envelope.put("checksum",bundle.get("checksum"));
  return envelope;
 }

 private static Map<String,Object> largeValueMapPin(int number,int conceptPadding){
  String id="urn:map:"+number, set=new UUID(1,number).toString();
  String target="urn:vocab@"+set;
  String codePrefix="e".repeat(124), conceptPrefix="urn:concept:"+"x".repeat(conceptPadding);
  var entries=new ArrayList<Map<String,String>>();
  for(int i=1000;i<2000;i++)entries.add(Map.of("code",codePrefix+i,"concept",conceptPrefix+i));
  var definition=Map.of("targetVocabulary",target,"entries",entries);
  return Map.of("semanticId",id,"semanticVersion","1","publicationSetId",set,"revisionId",new UUID(0,number).toString(),
      "valueMapRef",id+"@"+set,"targetVocabulary",target,"definition",definition,"definitionSha256",ValuePins.digest(definition));
 }

 private static List<String> pageQueries(String after,int... limits){
  return Arrays.stream(limits).mapToObj(limit->"limit="+limit+"&after="+after).toList();
 }

 private static Reply pageReply(List<byte[]> rows,String cursor){
  var chunks=new ArrayList<byte[]>();
  chunks.add(utf8("{\"items\":["));
  byte[] separator=utf8(",");
  for(byte[] row:rows){
   if(chunks.size()>1)chunks.add(separator);
   chunks.add(row);
  }
  chunks.add(utf8("],\"nextAfter\":\""+cursor+"\"}"));
  return new Reply(200,chunks);
 }

 private static byte[] utf8(String value){return value.getBytes(StandardCharsets.UTF_8);}

 private static byte[] pad(byte[] json,int size){
  byte[] body=Arrays.copyOf(json,size);
  Arrays.fill(body,json.length,size,(byte)' ');
  return body;
 }

 private static PublicationGatewayClient inMemoryClient(Path token,Function<HttpRequest,Reply> route) throws Exception {
  Files.writeString(token,"test-token");
  return new PublicationGatewayClient(PublishedActivationTest.JSON,"http://gateway.invalid",token.toString(),"tenant-a",(request,handler)->{
   var reply=route.apply(request);
   var subscriber=handler.apply(new InMemoryResponse(request,reply.status(),null));
   var subscription=new InMemorySubscription();
   subscriber.onSubscribe(subscription);
   for(byte[] chunk:reply.chunks()){
    subscriber.onNext(List.of(ByteBuffer.wrap(chunk)));
    if(subscription.cancelled)break;
   }
   if(!subscription.cancelled)subscriber.onComplete();
   return subscriber.getBody().toCompletableFuture().thenApply(body->new InMemoryResponse(request,reply.status(),body));
  });
 }

 private record Reply(int status,List<byte[]> chunks){}

 private static final class InMemorySubscription implements Flow.Subscription {
  private boolean cancelled;
  @Override public void request(long n){}
  @Override public void cancel(){cancelled=true;}
 }

 private record InMemoryResponse(HttpRequest request,int statusCode,byte[] body)
     implements HttpResponse<byte[]>, HttpResponse.ResponseInfo {
  @Override public HttpHeaders headers(){return HttpHeaders.of(Map.of(),(name,value)->true);}
  @Override public Optional<HttpResponse<byte[]>> previousResponse(){return Optional.empty();}
  @Override public Optional<javax.net.ssl.SSLSession> sslSession(){return Optional.empty();}
  @Override public URI uri(){return request.uri();}
  @Override public HttpClient.Version version(){return HttpClient.Version.HTTP_1_1;}
 }
}
