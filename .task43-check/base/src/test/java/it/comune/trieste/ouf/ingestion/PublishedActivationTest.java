package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class PublishedActivationTest {
 static final ObjectMapper JSON=new ObjectMapper();
 static Map<String,Object> envelope(String source,boolean once,long sequence){
  var policy=new LinkedHashMap<String,Object>(Map.of("timeZone","Europe/Rome","misfireToleranceSeconds",60,"sourceTimeoutSeconds",30,"maxRetryAttempts",3,"maxRetryElapsedSeconds",300,"incidentDedupWindowSeconds",300,"operationalVisibilityClass","TENANT_OPERATIONAL","operationalRetentionDays",30));
  var sync=new LinkedHashMap<String,Object>(Map.of("bootstrap","FULL_SNAPSHOT","incremental","NONE","retryBackoffSeconds",5,"operationalPolicy",policy));if(!once)sync.put("pollInterval","PT1M");
  var b=new LinkedHashMap<String,Object>();b.put("bundleId","bundle-"+source);b.put("bundleVersion",String.valueOf(sequence));b.put("source",Map.of("sourceId",source,"sourceKind",once?"INTERNAL_MANAGED":"EXTERNAL_API","acquisitionMode",once?"MANAGED":"PULL"));b.put("status","ACTIVE");b.put("createdFromOnboardingVersion",UUID.randomUUID().toString());b.put("syncProfile",sync);b.put("extractionProfile",Map.of("profileId","profile-"+source));b.put("bindingRef","gateway://source/"+source);b.put("semanticRefs",List.of("core@1"));b.put("semanticReferenceBindings",List.of(Map.of("semanticId","core","semanticVersion","1","revisionId","00000000-0000-0000-0000-000000000001","publicationSetId","00000000-0000-0000-0000-000000000002")));hash(b);
  return new LinkedHashMap<>(Map.of("publicationId",UUID.randomUUID().toString(),"sourceId",source,"onboardingVersionId",b.get("createdFromOnboardingVersion"),"publicationSequence",sequence,"tenantId","tenant-a","sourceStatus","ENABLED","checksum",b.get("checksum"),"bundle",b));
 }
 static void hash(Map<String,Object> b){try{b.remove("checksum");String value="sha256:"+HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(JSON.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS,true).writeValueAsBytes(b)));b.put("checksum",value);}catch(Exception e){throw new AssertionError(e);}}
 @Test void validatesPublisherHashTenantCadenceAndManagedOnce(){var pull=envelope("pull",false,1);assertThat(PublishedActivation.decode(pull,"tenant-a",JSON).intervalSeconds()).isEqualTo(60);assertThat(PublishedActivation.decode(envelope("file",true,1),"tenant-a",JSON).once()).isTrue();assertThatThrownBy(()->PublishedActivation.decode(pull,"other",JSON)).hasMessage("ING_PUBLICATION_INVALID");PublishedActivation.map(pull,"bundle").put("bundleVersion","tampered");assertThatThrownBy(()->PublishedActivation.decode(pull,"tenant-a",JSON)).hasMessage("ING_PUBLICATION_HASH_MISMATCH");}
 @Test void rejectsInventedPolicyAndPollingForManaged(){for(boolean once:List.of(true,false)){var e=envelope("source",once,1);var b=PublishedActivation.map(e,"bundle");var sync=PublishedActivation.map(b,"syncProfile");if(once)sync.put("pollInterval","PT1M");else sync.remove("operationalPolicy");hash(b);e.put("checksum",b.get("checksum"));assertThatThrownBy(()->PublishedActivation.decode(e,"tenant-a",JSON)).isInstanceOf(IllegalArgumentException.class);}}
}
