package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class PublicationGatewayClientTest {
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
}
