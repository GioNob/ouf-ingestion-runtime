package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.net.http.HttpHeaders;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ReferenceBatchTest {
  @TempDir Path directory;

  @DisplayName("ING-51B-BYTES")
  @Test void byteLimitIncludesUtf8EnvelopeAndCommas() throws Exception {
    try (var peer=new SemanticGatewayFixture(directory)) {
      var bundle=SemanticGatewayFixture.bundle(7);
      peer.client(Map.of()).preflight(bundle);
      int exact=peer.requestBytes.getFirst();
      peer.client(Map.of("reference-batch-bytes",String.valueOf(exact))).preflight(bundle);
      peer.client(Map.of("reference-batch-bytes",String.valueOf(exact-1))).preflight(bundle);
      assertThat(peer.batches.stream().map(List::size)).containsExactly(7,7,6,1);
      assertThat(peer.requestBytes.subList(2,4)).allMatch(n->n<exact);
    }
  }

  @DisplayName("ING-51B-413")
  @Test void coded413HalvesBatchAndIdentifiesOversizedSingleton() throws Exception {
    try (var peer=new SemanticGatewayFixture(directory)) {
      peer.maximum=2;
      var client=peer.client(Map.of());
      client.prepare(SemanticGatewayFixture.bundle(8));
      assertThat(peer.batches.stream().map(List::size)).containsExactly(8,4,2,2,2,2);
      peer.maximum=0;
      assertThatThrownBy(()->client.preflight(SemanticGatewayFixture.bundle(1)))
          .isInstanceOfSatisfying(ValuePins.BindingFailure.class,e->assertThat(e.index).isZero())
          .hasMessage("ING_SEMANTIC_REFERENCE_TOO_LARGE");
      int calls=peer.batches.size();
      assertThatThrownBy(()->peer.client(Map.of("reference-batch-bytes","10")).prepare(SemanticGatewayFixture.bundle(1)))
          .hasMessage("ING_SEMANTIC_REFERENCE_TOO_LARGE");
      assertThat(peer.batches).hasSize(calls);
    }
  }

  @DisplayName("ING-51B-VERSION")
  @Test void registryChangeRestartsFromFirstPinAndNeverCachesResults() throws Exception {
    try (var peer=new SemanticGatewayFixture(directory)) {
      peer.version=n->n==1?"before":"after";
      var client=peer.client(Map.of("reference-batch-items","2"));
      var bundle=SemanticGatewayFixture.bundle(3);
      client.prepare(bundle);
      assertThat(peer.batches).hasSize(4);
      assertThat(peer.batches.subList(0,2)).isEqualTo(peer.batches.subList(2,4));
      peer.version=n->"version-"+n;
      assertThatThrownBy(()->client.preflight(bundle)).hasMessage("ING_REFERENCE_BATCH_INCONSISTENT");
      assertThat(peer.batches).hasSize(10);
    }
  }

  @ParameterizedTest @ValueSource(strings={"missing","duplicate","index","echo","order"})
  @DisplayName("ING-51B-SHAPE")
  void corruptStructureHasFiniteWholeVerificationBudget(String fault) throws Exception {
    try (var peer=new SemanticGatewayFixture(directory)) {
      peer.change=rows->{
        switch(fault) {
          case "missing" -> rows.removeLast();
          case "duplicate" -> rows.set(1,rows.getFirst());
          case "index" -> rows.getFirst().put("index",1);
          case "echo" -> rows.getFirst().put("revisionId",UUID.randomUUID().toString());
          case "order" -> Collections.reverse(rows);
        }
      };
      assertThatThrownBy(()->peer.client(Map.of()).prepare(SemanticGatewayFixture.bundle(2)))
          .hasMessage("ING_REFERENCE_BATCH_INCONSISTENT");
      assertThat(peer.batches).hasSize(3);
    }
  }

  @ParameterizedTest @ValueSource(strings={"DATATYPE","VALUE_MAP"})
  @DisplayName("ING-51B-D084")
  void compactDigestAndLegacyDefinitionHaveEquivalentVerdicts(String kind) {
    Map<String,Object> definition=Map.of("name","è");
    for (String fault:List.of("valid","embedded","registry","missing","wrong-kind")) {
      var pin=new HashMap<String,Object>();
      if (kind.equals("VALUE_MAP")) pin.put("valueMapRef","urn:map@set");
      pin.put("definition",fault.equals("embedded")?Map.of("name","x"):definition);
      pin.put("definitionSha256",ValuePins.digest(definition));
      var compact=new HashMap<String,Object>();
      compact.put("artifact_type",fault.equals("wrong-kind")?"PROPERTY":kind);
      if (!fault.equals("missing")) compact.put("definitionSha256",ValuePins.digest(fault.equals("registry")?Map.of("name","y"):definition));
      var legacy=new HashMap<>(compact);
      legacy.put("definition",fault.equals("registry")?Map.of("name","y"):definition);
      var old=capturing(()->ValuePins.verifyBinding(pin,legacy));
      var batch=capturing(()->ValuePins.verifyBinding(pin,compact,true));
      assertThat(batch).isEqualTo(old);
      assertThat(batch==null).isEqualTo(fault.equals("valid"));
    }
  }

  @ParameterizedTest @ValueSource(ints={404,405})
  @DisplayName("ING-51B-LEGACY")
  void missingEndpointUsesPacedGetsUntilCapabilityExpires(int status) throws Exception {
    try(var peer=new SemanticGatewayFixture(directory)){
      var attempts=new java.util.concurrent.atomic.AtomicInteger();
      var gets=new java.util.concurrent.CopyOnWriteArrayList<Long>();
      peer.server.removeContext(ReferenceBatch.PATH);
      var pin=SemanticGatewayFixture.pins(1).getFirst();
      var resolved=Map.of("semantic_id",pin.get("semanticId"),"semantic_version","1","revision_id",pin.get("revisionId"),
          "publication_set_id",pin.get("publicationSetId"),"artifact_type","PROPERTY","status","ACTIVE");
      peer.server.createContext(ReferenceBatch.PATH,e->{try(e){
        if(attempts.incrementAndGet()==1){peer.reply(e,status,Map.of());return;}
        peer.reply(e,200,Map.of("contract","ouf.semantic.reference-batch.v1","registryVersion","restored",
            "results",List.of(Map.of("index",0,"semanticId",pin.get("semanticId"),"revisionId",pin.get("revisionId"),
                "publicationSetId",pin.get("publicationSetId"),"outcome","RESOLVED","resolved",resolved))));
      }});
      peer.server.createContext("/api/semantic/v1/references:resolve",e->{try(e){
        gets.add(System.nanoTime());
        peer.reply(e,200,resolved);
      }});
      var client=peer.client(Map.of("reference-fallback-seconds","1","references-per-minute","300"));
      var bundle=SemanticGatewayFixture.bundle(1);
      client.prepare(bundle);client.preflight(bundle);
      assertThat(attempts).hasValue(1);
      assertThat(gets).hasSize(2);
      assertThat(Duration.ofNanos(gets.getLast()-gets.getFirst())).isGreaterThan(Duration.ofMillis(150));
      Thread.sleep(Duration.ofMillis(1050));
      client.prepare(bundle);
      assertThat(attempts).hasValue(2);assertThat(gets).hasSize(2);
    }
  }

  @DisplayName("ING-51B-RETRY")
  @Test void dateRetryAfterSurvivesProgressWithoutAccumulatedBackoff(){
    var target=ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(20);
    var headers=HttpHeaders.of(Map.of("Retry-After",List.of(target.format(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME))),(k,v)->true);
    Duration wait=new GatewayPacing(k->null).retryDelay(null,headers);
    assertThat(RetryPolicy.afterProgress(wait)).isBetween(RetryPolicy.seconds(wait),24L);
    assertThat(RetryPolicy.delay(5,0,wait)).isGreaterThanOrEqualTo(900);
    assertThat(RetryPolicy.afterProgress(Duration.ofSeconds(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
  }
  @Test @DisplayName("ING-51B2-WAIT")
  void fractionalResetUsesEitherHeaderAndRetryAfterTakesPrecedence() {
    Instant now=Instant.parse("2026-11-22T12:00:00.250Z");
    long cap=Duration.ofSeconds(14).toNanos(), pace=Duration.ofMillis(250).toNanos();
    for(String prefix:List.of("X-RateLimit-","RateLimit-")){
      for(String value:List.of("12.5",(now.getEpochSecond()+12)+".75")){
        var fields=new HashMap<String,List<String>>();
        fields.put(prefix+"Reset",List.of(value));
        fields.put(prefix+"Remaining",List.of("0"));
        var wait=RetryPolicy.serverWait(HttpHeaders.of(fields,(k,v)->true),now,pace,cap);
        assertThat(wait).isBetween(Duration.ofMillis(12500),Duration.ofSeconds(14));
        assertThat(RetryPolicy.afterProgress(wait)).isBetween(13L,14L);
        for(String preferred:List.of("2","Sun, 22 Nov 2026 12:00:02 GMT")){
          fields.put("Retry-After",List.of(preferred));
          assertThat(RetryPolicy.serverWait(HttpHeaders.of(fields,(k,v)->true),now,pace,cap))
              .isBetween(Duration.ofMillis(preferred.equals("2")?2000:1750),Duration.ofMillis(2400));
        }
      }
    }
    var excessive=HttpHeaders.of(Map.of("Retry-After",List.of("9999999999999999999999")),(k,v)->true);
    assertThat(RetryPolicy.serverWait(excessive,now,pace,cap)).isEqualTo(Duration.ofSeconds(14));
    assertThat(RetryPolicy.serverWait(GatewayPacing.NO_HEADERS,now,pace,cap)).isBetween(Duration.ofMillis(250),Duration.ofMillis(300));
  }

  @ParameterizedTest @ValueSource(strings={"semantic_version","status","digest"})
  @DisplayName("ING-51B2-MISMATCH")
  void mismatchedResolutionIdentifiesThePin(String fault) throws Exception {
    try(var peer=new SemanticGatewayFixture(directory)){
      var pin=SemanticGatewayFixture.pins(3).getLast();
      peer.change=rows->{
        var row=rows.getLast();
        if(!pin.get("semanticId").equals(row.get("semanticId")))return;
        @SuppressWarnings("unchecked") var actual=new HashMap<>((Map<String,Object>)row.get("resolved"));
        if(fault.equals("digest")){
          actual.put("artifact_type","DATATYPE");
          actual.put("definitionSha256","0".repeat(64));
        }else actual.put(fault,"different");
        row.put("resolved",actual);
      };
      var bindings=new ArrayList<>(SemanticGatewayFixture.pins(3));
      if(fault.equals("digest")){
        var bound=new HashMap<>(pin);
        var definition=Map.of("components",List.of(Map.of("name","field","type","string","required",false,"cardinality","ONE")));
        bound.put("definition",definition);bound.put("definitionSha256",ValuePins.digest(definition));
        bindings.set(2,bound);
      }
      var bundle=new ExecutionBundle("fixture","1","hash","places","MANAGED",null,Map.of(
          "publishedBundle",Map.of("semanticReferenceBindings",bindings),"pinnedReferences",SemanticGatewayFixture.refs(bindings)));
      assertThatThrownBy(()->peer.client(Map.of("reference-batch-items","2")).preflight(bundle))
          .isInstanceOfSatisfying(ValuePins.BindingFailure.class,e->assertThat(e.detail()).isEqualTo(Map.of(
              "code","ING_SEMANTIC_REFERENCE_MISMATCH","index",2,"semanticId",pin.get("semanticId"),"revisionId",pin.get("revisionId"))));
      assertThat(peer.batches).hasSize(2);
    }
  }

  @ParameterizedTest @ValueSource(booleans={false,true})
  @DisplayName("ING-51B2-HTTP429")
  void semantic429SharesOneWaitWithItsLane(boolean retryAfter) throws Exception {
    try(var peer=new SemanticGatewayFixture(directory)){
      peer.server.removeContext(ReferenceBatch.PATH);
      peer.server.createContext(ReferenceBatch.PATH,e->{try(e){
        var headers=e.getResponseHeaders();
        headers.set("X-RateLimit-Limit","60");
        headers.set("X-RateLimit-Remaining","0");
        headers.set("X-RateLimit-Reset","12.5");
        if(retryAfter)headers.set("Retry-After","5");
        peer.reply(e,429,Map.of());
      }});
      var pacing=new GatewayPacing(k->k.endsWith("server-wait-max-seconds")?"14":null);
      var client=new PublicationGatewayClient(peer.json,peer.url,peer.token.toString(),"tenant-a",pacing);
      var failure=catchThrowableOfType(()->client.preflight(SemanticGatewayFixture.bundle(1)),RuntimePorts.GatewayFailure.class);
      assertThat(failure.safeCode()).isEqualTo("ING_ACTIVATION_GATEWAY_429");
      var wait=failure.retryAfter().orElseThrow();
      assertThat(wait).isBetween(Duration.ofMillis(retryAfter?5000:12500),Duration.ofSeconds(retryAfter?6:14));
      var held=catchThrowableOfType(pacing.references::acquire,RuntimePorts.GatewayFailure.class);
      assertThat(held.safeCode()).isEqualTo("ING_GATEWAY_PACING_WAIT");
      assertThat(held.retryAfter().orElseThrow()).isBetween(wait.minusSeconds(1),wait);
    }
  }

  @ParameterizedTest @ValueSource(strings={"429","503","timeout"})
  @DisplayName("ING-51B2-TRANSIENT")
  void transientOnSecondBlockDiscardsProvisionalItemFailure(String fault) throws Exception {
    try(var peer=new SemanticGatewayFixture(directory)){
      var calls=new java.util.concurrent.atomic.AtomicInteger();
      peer.server.removeContext(ReferenceBatch.PATH);
      peer.server.createContext(ReferenceBatch.PATH,e->{
        if(calls.incrementAndGet()!=2){peer.batch(e);return;}
        try(e){
          if(fault.equals("timeout")){
            try{Thread.sleep(Duration.ofMillis(5500));}
            catch(InterruptedException cancelled){Thread.currentThread().interrupt();}
          }else peer.reply(e,Integer.parseInt(fault),Map.of());
        }
      });
      var client=peer.client(Map.of("reference-batch-items","2"));
      var bundle=SemanticGatewayFixture.bundle(3);
      peer.revoked=SemanticGatewayFixture.id(1);
      assertThatThrownBy(()->client.preflight(bundle)).isInstanceOfSatisfying(RuntimePorts.GatewayFailure.class,e->{
        var cause=RunCoordinator.classify(e);
        assertThat(cause.errorClass()).isEqualTo(AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
        assertThat(cause.reference()).isNull();
        assertThat(cause.code()).isEqualTo(fault.equals("timeout")?"ING_ACTIVATION_TIMEOUT":"ING_ACTIVATION_GATEWAY_"+fault);
      });
      peer.revoked=null;peer.version=n->"fresh";
      client.preflight(bundle);
      assertThat(calls).hasValue(4);
      assertThat(peer.batches.stream().map(List::size)).containsExactly(2,2,1);
      assertThat(peer.batches.get(1)).isEqualTo(peer.batches.getFirst());
    }
  }

  private static String capturing(Runnable check) {
    try { check.run();return null; } catch (IllegalArgumentException invalid) { return invalid.getMessage(); }
  }

  @Test @DisplayName("ING-51B-CONCEPT")
  void conceptMembershipIsVerifiedFromTheBatchWithoutPerElementGets() throws Exception {
    try(var peer=new SemanticGatewayFixture(directory)){
      var pins=new ArrayList<Map<String,Object>>();
      for(var base:SemanticGatewayFixture.pins(64)){
        var pin=new HashMap<String,Object>(base);
        pin.put("definition",Map.of("vocabularyRef","urn:places"));
        pins.add(pin);
      }
      var bundle=new ExecutionBundle("fixture","1","hash","places","MANAGED",null,Map.of(
          "publishedBundle",Map.of("semanticReferenceBindings",pins),
          "pinnedReferences",pins.stream().map(p->p.get("semanticId")+"@1").toList()));
      var vocabulary=new java.util.concurrent.atomic.AtomicReference<>("urn:places");
      var withDefinition=new java.util.concurrent.atomic.AtomicBoolean(true);
      peer.change=rows->{
        for(int i=0;i<rows.size();i++){
          var pin=pins.get(i);
          var compact=new HashMap<String,Object>(Map.of("semantic_id",pin.get("semanticId"),"semantic_version","1",
              "revision_id",pin.get("revisionId"),"publication_set_id",pin.get("publicationSetId"),
              "status","ACTIVE","artifact_type","CONCEPT"));
          if(withDefinition.get())compact.put("definition",Map.of("vocabularyRef",vocabulary.get()));
          rows.get(i).put("resolved",compact);
        }
      };
      var gets=new java.util.concurrent.atomic.AtomicInteger();
      peer.server.createContext("/api/semantic/v1/references:resolve",e->{try(e){gets.incrementAndGet();peer.reply(e,500,Map.of());}});
      var client=peer.client(Map.of());
      client.prepare(bundle);
      assertThat(gets).hasValue(0);
      vocabulary.set("urn:other");
      assertThatThrownBy(()->client.prepare(bundle)).hasMessage("ING_VALUE_DEFINITION_INVALID");
      assertThat(gets).hasValue(0);
      // A CONCEPT row without its definition violates the batch contract: never a silent per-element GET.
      vocabulary.set("urn:places");
      withDefinition.set(false);
      assertThatThrownBy(()->client.prepare(bundle)).hasMessage("ING_REFERENCE_BATCH_INCONSISTENT");
      assertThat(gets).hasValue(0);
    }
  }

  @DisplayName("ING-51B-QUOTA")
  @Test void publishedWindowSlowsOnlyItsLaneBeforeExhaustion() {
    for (String prefix:List.of("RateLimit-","X-RateLimit-")) {
      var pacing=new GatewayPacing(k->"1000");
      String reset=prefix.startsWith("X")?(Instant.now().getEpochSecond()+50)+".5":"50.5";
      pacing.lake.observeQuota(HttpHeaders.of(Map.of(prefix+"Remaining",List.of("2.5"),prefix+"Reset",List.of(reset)),(k,v)->true));
      assertThatThrownBy(pacing.lake::acquire).isInstanceOfSatisfying(RuntimePorts.GatewayFailure.class,
          e->assertThat(e.retryAfter().orElseThrow()).isBetween(Duration.ofSeconds(18),Duration.ofSeconds(21)));
      pacing.handoff.acquire();
      pacing.references.acquire();
    }
  }
}
