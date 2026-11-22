package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** A verification owns its version, partitions and results. Only endpoint availability survives it. */
final class ReferenceBatch {
  static final String PATH = "/api/semantic/v1/references:resolve-batch";
  private static final String MISMATCH = "ING_SEMANTIC_BINDING_MISMATCH";
  private static final String TOO_LARGE = "ING_SEMANTIC_REFERENCE_TOO_LARGE";
  private static final byte[] PREFIX = "{\"references\":[".getBytes(StandardCharsets.US_ASCII);
  private final GatewayPacing settings;
  private final ObjectMapper json;
  private final PublicationGatewayClient gateway;
  private volatile long fallbackUntil = System.nanoTime();

  ReferenceBatch(ObjectMapper json, PublicationGatewayClient gateway, GatewayPacing settings) {
    this.json = json;
    this.gateway = gateway;
    this.settings = settings;
  }

  record Pin(int index, Map<?,?> binding, String id, String version, String revision, String publication, byte[] bytes) {
    ValuePins.BindingFailure failure(String runCode, String itemCode) {
      return new ValuePins.BindingFailure(runCode, itemCode, index, id, revision);
    }
    boolean matches(Map<?,?> row) {
      return id.equals(row.get("semanticId")) && revision.equals(row.get("revisionId")) && publication.equals(row.get("publicationSetId"));
    }
    void verifyIdentity(Map<?,?> row, PublicationGatewayClient.Resolution mode) {
      if (!id.equals(row.get("semantic_id")) || !version.equals(row.get("semantic_version")) ||
          !revision.equals(row.get("revision_id")) || !publication.equals(row.get("publication_set_id")) || !mode.accepts(row))
        throw failure(MISMATCH, ValuePins.BindingFailure.MISMATCH);
    }
    void verifyDefinition(Map<?,?> row, boolean compact) {
      try {
        ValuePins.verifyBinding(binding, row, compact);
      } catch (IllegalArgumentException invalid) { throw failure(invalid.getMessage(), ValuePins.BindingFailure.MISMATCH); }
    }
  }

  void verify(List<?> bindings, Collection<?> expected, PublicationGatewayClient.Resolution mode) {
    List<Pin> pins = new ArrayList<>(bindings.size());
    var remaining = new HashSet<>(expected);
    if (remaining.size() != expected.size()) throw new IllegalArgumentException(MISMATCH);
    var tuples = new HashSet<Map<String,String>>();
    for (Object raw : bindings) {
      if (!(raw instanceof Map<?,?> map)) throw new IllegalArgumentException("ING_EXACT_SEMANTIC_BINDINGS_REQUIRED");
      @SuppressWarnings("unchecked") var binding = (Map<String,Object>)map;
      String id = PublishedActivation.text(binding,"semanticId"), version = PublishedActivation.text(binding,"semanticVersion");
      String revision = UUID.fromString(PublishedActivation.text(binding,"revisionId")).toString();
      String publication = UUID.fromString(PublishedActivation.text(binding,"publicationSetId")).toString();
      var tuple = Map.of("semanticId",id,"revisionId",revision,"publicationSetId",publication);
      if (!remaining.remove(id+"@"+version) || !tuples.add(tuple)) throw new ValuePins.BindingFailure(MISMATCH,ValuePins.BindingFailure.MISMATCH,pins.size(),id,revision);
      try {
        pins.add(new Pin(pins.size(), binding, id, version, revision, publication, json.writeValueAsBytes(tuple)));
      } catch (JsonProcessingException invalid) { throw new IllegalArgumentException("ING_REFERENCE_BATCH_ENCODING_INVALID"); }
    }
    if (!remaining.isEmpty()) throw new IllegalArgumentException(MISMATCH);

    if (fallbackUntil - System.nanoTime() <= 0) {
      try {
        for (int attempt = 0; attempt < 3; attempt++)
          if (verifySnapshot(pins, mode)) return;
        throw new IllegalArgumentException("ING_REFERENCE_BATCH_INCONSISTENT");
      } catch (EndpointUnavailable unavailable) {
        fallbackUntil = System.nanoTime() + settings.fallbackTtl.toNanos();
      }
    }
    for (Pin pin : pins) verifyLegacy(pin, mode);
  }

  private void verifyLegacy(Pin pin, PublicationGatewayClient.Resolution mode) {
    try {
      var resolved = gateway.resolveOne(pin);
      pin.verifyIdentity(resolved, mode);
      pin.verifyDefinition(resolved, false);
    }
    catch (RuntimePorts.GatewayFailure error) {
      if ("ING_ACTIVATION_GATEWAY_404".equals(error.safeCode())) throw pin.failure(MISMATCH, ValuePins.BindingFailure.UNRESOLVABLE);
      throw error;
    }
  }
  static final class EndpointUnavailable extends RuntimeException {}
  static final class TooLarge extends RuntimeException {}
  static final class InvalidReply extends RuntimeException {}

  private boolean verifySnapshot(List<Pin> pins, PublicationGatewayClient.Resolution mode) {
    int cap = settings.batchItems;
    String version = null;
    ValuePins.BindingFailure firstFailure = null;
    for (int start = 0; start < pins.size();) {
      var body = new ByteArrayOutputStream();
      body.writeBytes(PREFIX);
      int end = start;
      while (end < pins.size() && end - start < cap) {
        byte[] tuple = pins.get(end).bytes();
        int comma = end == start ? 0 : 1;
        if ((long)body.size() + comma + tuple.length + 2 > settings.batchBytes) break;
        if (comma != 0) body.write(',');
        body.writeBytes(tuple);
        end++;
      }
      if (end == start) throw pins.get(start).failure(TOO_LARGE, TOO_LARGE);
      body.write(']'); body.write('}');
      Map<String,Object> page;
      try { page = gateway.resolveBatch(body.toByteArray()); }
      catch (InvalidReply invalid) { return false; }
      catch (TooLarge oversized) {
        if (end - start == 1) throw pins.get(start).failure(TOO_LARGE, TOO_LARGE);
        cap = (end - start) / 2;
        continue;
      }
      if (!"ouf.semantic.reference-batch.v1".equals(page.get("contract")) ||
          !(page.get("registryVersion") instanceof String marker) || marker.isBlank() ||
          version != null && !version.equals(marker) ||
          !(page.get("results") instanceof List<?> rows) || rows.size() != end - start) return false;
      version = marker;
      for (int i = 0; i < rows.size(); i++) {
        Pin pin = pins.get(start + i);
        if (!(rows.get(i) instanceof Map<?,?> row) || !Integer.valueOf(i).equals(row.get("index")) || !pin.matches(row)) return false;
        if ("NOT_RESOLVABLE".equals(row.get("outcome"))) {
          if (firstFailure == null) firstFailure = pin.failure(MISMATCH, ValuePins.BindingFailure.UNRESOLVABLE);
        } else if ("RESOLVED".equals(row.get("outcome")) && row.get("resolved") instanceof Map<?,?> resolved) {
          try {
            pin.verifyIdentity(resolved, mode);
            // The batch carries the CONCEPT definition (vocabularyRef) exactly as the GET: membership is
            // verified from the batch. A CONCEPT row without it violates the contract: reread the whole set.
            if ("CONCEPT".equals(resolved.get("artifact_type")) && !(resolved.get("definition") instanceof Map<?,?>))
              return false;
            pin.verifyDefinition(resolved, true);
          } catch (ValuePins.BindingFailure invalid) { if (firstFailure == null) firstFailure = invalid; }
        } else return false;
      }
      start = end;
    }
    if (firstFailure != null) throw firstFailure;
    return true;
  }
}
