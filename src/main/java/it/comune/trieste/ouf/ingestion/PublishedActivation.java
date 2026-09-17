package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.*;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;

/** Verifies the publisher's immutable bytes/identity before adapting its runtime shape. */
public record PublishedActivation(UUID publicationId,long sequence,String sourceId,String tenantId,boolean enabled,boolean once,int intervalSeconds,ExecutionBundle execution) {
  public static PublishedActivation decode(Map<String,Object> envelope,String tenant,ObjectMapper json){
    try{
      Map<String,Object> b=map(envelope,"bundle");String source=text(envelope,"sourceId");Map<String,Object> sourceConfig=map(b,"source");
      if(!tenant.equals(text(envelope,"tenantId"))||!source.equals(text(sourceConfig,"sourceId"))||!"ACTIVE".equals(text(b,"status"))||!text(envelope,"onboardingVersionId").equals(text(b,"createdFromOnboardingVersion")))throw invalid();
      String checksum=text(b,"checksum");var unsigned=new TreeMap<>(b);unsigned.remove("checksum");String actual="sha256:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS,true).writeValueAsBytes(unsigned)));
      if(!checksum.equals(actual)||!checksum.equals(text(envelope,"checksum")))throw new IllegalArgumentException("ING_PUBLICATION_HASH_MISMATCH");
      String status=text(envelope,"sourceStatus");if(!Set.of("REGISTERED","ENABLED","DISABLED").contains(status))throw invalid();
      String acquisition=text(sourceConfig,"acquisitionMode");boolean once="MANAGED".equals(acquisition);if(!once&&!"PULL".equals(acquisition))throw invalid();
      Map<String,Object> sync=map(b,"syncProfile"),extraction=map(b,"extractionProfile");String runtimeMode=once?"MANAGED":"PULL";
      int interval=0;
      if(once){if(sync.get("pollInterval")!=null)throw new IllegalArgumentException("ING_MANAGED_POLLING_FORBIDDEN");}
      else{
        Duration cadence=Duration.parse(text(sync,"pollInterval"));if(cadence.getNano()!=0||cadence.getSeconds()<60||cadence.getSeconds()>2678400)throw new IllegalArgumentException("ING_PUBLICATION_CADENCE_INVALID");interval=Math.toIntExact(cadence.getSeconds());
        Map<String,Object> policy=map(sync,"operationalPolicy");ZoneId.of(text(policy,"timeZone"));bounded(policy,"misfireToleranceSeconds",1,86400);bounded(policy,"sourceTimeoutSeconds",1,300);bounded(policy,"maxRetryAttempts",0,100);bounded(sync,"retryBackoffSeconds",1,86400);bounded(policy,"maxRetryElapsedSeconds",1,86400);bounded(policy,"incidentDedupWindowSeconds",1,86400);bounded(policy,"operationalRetentionDays",30,3650);
        if(!Set.of("PUBLIC_OPERATIONAL","TENANT_OPERATIONAL","RESTRICTED_OPERATIONAL","SECURITY_SENSITIVE").contains(text(policy,"operationalVisibilityClass")))throw invalid();
      }
      List<String> refs=new ArrayList<>();Object raw=b.get("semanticRefs");if(!(raw instanceof List<?> list)||list.isEmpty()||list.size()>100)throw new IllegalArgumentException("ING_PREFLIGHT_REFS_REQUIRED");for(Object ref:list){if(!(ref instanceof String s)||!s.matches("[^@\\s]+@[^@\\s]+"))throw new IllegalArgumentException("ING_PREFLIGHT_EXACT_REF_REQUIRED");refs.add(s);}
      var config=new LinkedHashMap<String,Object>();config.put("publishedBundle",b);config.put("publicationId",text(envelope,"publicationId"));config.put("publicationSequence",number(envelope,"publicationSequence"));config.put("pinnedReferences",refs);config.put("syncProfile",sync);config.put("extractionProfile",extraction);
      String binding=once?null:text(b,"bindingRef");
      long sequence=number(envelope,"publicationSequence");if(sequence<1)throw invalid();
      return new PublishedActivation(UUID.fromString(text(envelope,"publicationId")),sequence,source,tenant,!status.equals("DISABLED"),once,interval,new ExecutionBundle(text(b,"bundleId"),text(b,"bundleVersion"),checksum,source,runtimeMode,binding,config));
    }catch(IllegalArgumentException e){if(e.getMessage()!=null&&e.getMessage().startsWith("ING_"))throw e;throw invalid();}catch(Exception e){throw invalid();}
  }
  @SuppressWarnings("unchecked") static Map<String,Object> map(Map<String,Object> p,String k){if(!(p.get(k) instanceof Map<?,?> m))throw invalid();return (Map<String,Object>)m;}
  static String text(Map<String,Object> p,String k){if(!(p.get(k) instanceof String s)||s.isBlank())throw invalid();return s;}
  static long number(Map<String,Object> p,String k){if(!(p.get(k) instanceof Number n)||n.doubleValue()!=n.longValue())throw invalid();return n.longValue();}
  private static void bounded(Map<String,Object> p,String k,long min,long max){long n=number(p,k);if(n<min||n>max)throw invalid();}
  private static IllegalArgumentException invalid(){return new IllegalArgumentException("ING_PUBLICATION_INVALID");}
}
