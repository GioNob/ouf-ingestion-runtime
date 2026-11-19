package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Dedicated minimized projection; historical/raw evidence APIs remain unchanged. */
@RestController
@RequestMapping("/api/internal/v1/ingestion/operations")
public class AcquisitionEvidenceApi {
  private final AcquisitionEvidenceService service;
  private final TrustedAuthorizationContext authorization;
  public AcquisitionEvidenceApi(AcquisitionEvidenceService service,TrustedAuthorizationContext authorization){this.service=service;this.authorization=authorization;}
  @PostMapping(value="/acquisition",consumes="application/json",produces="application/json")
  public Map<String,Object> read(@RequestBody byte[] body,HttpServletRequest request){
    Query q;
    try{if(body.length>4096)throw invalid();
      var parser=new com.fasterxml.jackson.databind.ObjectMapper().enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
      q=decode(parser.readTree(body));
    }catch(java.io.IOException invalid){throw invalid();}
    return service.read(q,authorization.owner(request,"ingestion.lineage.read"));
  }
  public record Query(String sourceId,UUID runId,int limit,UUID afterLineageId){
    public Query{
      if(sourceId==null||sourceId.isBlank()||sourceId.length()>200||sourceId.chars().anyMatch(Character::isISOControl)
          ||runId==null||runId.equals(new UUID(0,0))||limit<1||limit>100||new UUID(0,0).equals(afterLineageId))throw invalid();
    }
  }
  static Query decode(JsonNode body){
    if(body==null||!body.isObject()||!body.hasNonNull("sourceId")||!body.hasNonNull("runId"))throw invalid();
    for(var fields=body.fieldNames();fields.hasNext();)if(!Set.of("sourceId","runId","limit","afterLineageId").contains(fields.next()))throw invalid();
    if(!body.get("sourceId").isTextual()||!body.get("runId").isTextual()
        ||body.has("limit")&&(!body.get("limit").isIntegralNumber()||!body.get("limit").canConvertToInt())
        ||body.has("afterLineageId")&&!body.get("afterLineageId").isTextual())throw invalid();
    try{return new Query(body.get("sourceId").textValue(),uuid(body.get("runId").textValue()),
        body.has("limit")?body.get("limit").intValue():50,body.has("afterLineageId")?uuid(body.get("afterLineageId").textValue()):null);}
    catch(IllegalArgumentException bad){throw invalid();}
  }
  private static UUID uuid(String value){UUID result=UUID.fromString(value);if(!result.toString().equals(value))throw invalid();return result;}
  static ResponseStatusException invalid(){return new ResponseStatusException(HttpStatus.BAD_REQUEST,"ING_ACQUISITION_QUERY_INVALID");}
}
