package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/** Contract peer on loopback. Both clients read the same serialized tuples as a Gateway would. */
final class SemanticGatewayFixture implements AutoCloseable {
  private static final Map<String,Map<String,Object>> REGISTRY=pins(1024).stream().collect(
      java.util.stream.Collectors.toUnmodifiableMap(pin->(String)pin.get("semanticId"),pin->pin));
  final ObjectMapper json = new ObjectMapper();
  final HttpServer server;
  final Path token;
  final String url;
  final List<List<String>> batches = new CopyOnWriteArrayList<>();
  final List<Integer> requestBytes = new CopyOnWriteArrayList<>();
  volatile int maximum = 1000;
  volatile String revoked;
  volatile java.util.function.IntFunction<String> version = call -> "registry-1";
  volatile java.util.function.Consumer<List<Map<String,Object>>> change = rows -> {};

  SemanticGatewayFixture(Path directory) throws IOException {
    token = Files.writeString(directory.resolve("token"),"fixture-workload");
    server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    url = "http://127.0.0.1:" + server.getAddress().getPort();
    server.createContext(ReferenceBatch.PATH,this::batch);
    server.start();
  }
  PublicationGatewayClient client(Map<String,String> limits) {
    var settings = new GatewayPacing(key -> limits.getOrDefault(key.substring(key.lastIndexOf('.')+1),
        key.endsWith("per-minute") ? "60000" : null));
    return new PublicationGatewayClient(json,url,token.toString(),"tenant-a",settings);
  }
  static String id(int i) { return "https://example.test/proprietà/" + i; }
  static List<Map<String,Object>> pins(int count) {
    var pins = new ArrayList<Map<String,Object>>();
    for (int i=0;i<count;i++) pins.add(Map.of("semanticId",id(i),"semanticVersion","1",
        "revisionId",new UUID(0,i+1).toString(),"publicationSetId",new UUID(1,1).toString()));
    return pins;
  }
  static List<String> refs(List<Map<String,Object>> pins) {
    return pins.stream().map(pin -> pin.get("semanticId")+"@1").toList();
  }
  static ExecutionBundle bundle(int count) {
    var pins = pins(count);
    return new ExecutionBundle("fixture","1","hash","places","MANAGED",null,
        Map.of("publishedBundle",Map.of("semanticReferenceBindings",pins),"pinnedReferences",refs(pins)));
  }
  void batch(HttpExchange exchange) throws IOException {
    try (exchange) {
      if (!"Bearer fixture-workload".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
        reply(exchange,403,Map.of()); return;
      }
      if (!"POST".equals(exchange.getRequestMethod())) { reply(exchange,405,Map.of()); return; }
      byte[] body = exchange.getRequestBody().readAllBytes();
      requestBytes.add(body.length);
      JsonNode refs = json.readTree(body).required("references");
      List<String> ids = new ArrayList<>();
      var seen=new HashSet<JsonNode>();
      List<Map<String,Object>> rows = new ArrayList<>();
      for (var ref : refs) {
        if (!seen.add(ref) || !ref.path("semanticId").isTextual() || !ref.path("revisionId").isTextual() || !ref.path("publicationSetId").isTextual()) {
          reply(exchange,400,Map.of("code","SEM_REFERENCE_INVALID","index",ids.size())); return;
        }
        var id = ref.required("semanticId").textValue();
        var revision = ref.required("revisionId").textValue();
        var set = ref.required("publicationSetId").textValue();
        var row = new HashMap<String,Object>(Map.of("index",ids.size(),"semanticId",id,"revisionId",revision,"publicationSetId",set));
        ids.add(id);
        var pin=REGISTRY.get(id);
        boolean visible=pin!=null && !id.equals(revoked) && revision.equals(pin.get("revisionId")) && set.equals(pin.get("publicationSetId"));
        row.put("outcome",visible?"RESOLVED":"NOT_RESOLVABLE");
        if (visible) row.put("resolved",Map.of("semantic_id",id,"semantic_version","1","revision_id",revision,
            "publication_set_id",set,"status","ACTIVE","artifact_type","PROPERTY","content_hash","fixture","publication_checksum","fixture"));
        rows.add(row);
      }
      batches.add(List.copyOf(ids));
      if (rows.size()>maximum || body.length>262144) {
        reply(exchange,413,Map.of("code","SEM_REFERENCE_BATCH_TOO_LARGE")); return;
      }
      change.accept(rows);
      reply(exchange,200,Map.of("contract","ouf.semantic.reference-batch.v1","registryVersion",version.apply(batches.size()),"results",rows));
    }
  }
  void reply(HttpExchange exchange,int status,Object value) throws IOException {
    byte[] bytes=json.writeValueAsBytes(value);
    exchange.getResponseHeaders().set("Content-Type","application/json");
    exchange.sendResponseHeaders(status,bytes.length);
    exchange.getResponseBody().write(bytes);
  }
  public void close() { server.stop(0); }
}
