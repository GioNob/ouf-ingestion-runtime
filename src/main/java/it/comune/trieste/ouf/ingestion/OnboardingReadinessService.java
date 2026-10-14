package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/** One bounded compatibility attempt. The source owner retains the durable attempt budget. */
@Service
@ConditionalOnProperty(name={"ouf.ingestion.activation.enabled","ouf.ingestion.execution.enabled"},havingValue="true")
public class OnboardingReadinessService {
  private final ObjectMapper json;
  private final PublicationGatewayClient gateway;
  private final FrozenConfigurationProbe probe;
  private final ExecutorService workers=new ThreadPoolExecutor(0,2,30,TimeUnit.SECONDS,
      new SynchronousQueue<>(),r->{var t=new Thread(r,"onboarding-readiness");t.setDaemon(true);return t;},new ThreadPoolExecutor.AbortPolicy());
  public OnboardingReadinessService(ObjectMapper json,PublicationGatewayClient gateway,ExecutionGatewayClient objects){
    this.json=json;this.gateway=gateway;this.probe=new FrozenConfigurationProbe(json,objects,gateway);
  }
  @PreDestroy public void close(){workers.shutdownNow();}
  public Map<String,Object> verify(String source,UUID version,String hash,Runnable authorize){
    Future<Map<String,Object>> future;
    try{future=workers.submit(()->attempt(source,version,hash,authorize));}
    catch(RejectedExecutionException pressure){throw transientFailure("ING_READINESS_CAPACITY_PAUSED");}
    try{return future.get(30,TimeUnit.SECONDS);}
    catch(TimeoutException timeout){future.cancel(true);throw transientFailure("ING_READINESS_DEADLINE");}
    catch(InterruptedException cancelled){future.cancel(true);Thread.currentThread().interrupt();throw transientFailure("ING_READINESS_CANCELLED");}
    catch(ExecutionException failure){if(failure.getCause() instanceof RuntimeException e)throw e;throw new IllegalArgumentException("ING_READINESS_UNPROVEN");}
  }
  Map<String,Object> attempt(String source,UUID version,String hash,Runnable authorize){
    authorize.run();
    var current=gateway.frozenConfiguration(source,version);
    if(!source.equals(current.get("source_id"))||!"APPROVED".equals(current.get("state"))||!version.toString().equals(String.valueOf(current.get("onboarding_version_id")))
        ||!hash.equals(current.get("configuration_hash")))throw new IllegalArgumentException("ING_READINESS_FROZEN_IDENTITY_CHANGED");
    Map<String,Object> configuration=PublishedActivation.map(current,"configuration");
    if(!hash.equals(FrozenConfigurationProbe.configurationHash(json,configuration)))throw new IllegalArgumentException("ING_READINESS_CONFIGURATION_HASH_MISMATCH");
    var retained=gateway.compatibility(source,version,hash);
    if(Boolean.TRUE.equals(retained.get("compatible"))){var row=PublishedActivation.map(retained,"attestation");verifyAttestation(row,version,hash);authorize.run();requireRunning();return result(source,version,hash,row,true,null);}
    if(!"ABSENT".equals(retained.get("status")))throw new IllegalArgumentException("ING_READINESS_EXISTING_ATTESTATION_REJECTED");
    var candidate=new LinkedHashMap<String,Object>();candidate.put("sourceId",source);candidate.put("onboardingVersionId",version.toString());
    candidate.put("configurationHash",hash);candidate.put("configuration",configuration);candidate.put("state","APPROVED");candidate.put("version",current.get("version"));
    var proof=probe.probe(candidate);
    authorize.run();requireRunning();
    // The owner atomically checks this exact immutable hash and deduplicates retries. A lost ACK is reconciled by GET on the next bounded attempt.
    var attested=gateway.attestCompatibility(source,version,hash,((Number)proof.get("validatedRows")).longValue());
    verifyAttestation(attested,version,hash);requireRunning();
    return result(source,version,hash,attested,false,proof.get("validatedRows"));
  }
  private static void verifyAttestation(Map<String,Object> row,UUID version,String hash){
    if(!Boolean.TRUE.equals(row.get("compatible"))||!version.toString().equals(String.valueOf(row.get("onboarding_version_id")))
        ||!hash.equals(row.get("configuration_hash"))||!"INGESTION_RUNTIME".equals(row.get("consumer"))||row.get("attestation_id")==null)
      throw new IllegalArgumentException("ING_READINESS_ATTESTATION_UNPROVEN");
  }
  private static Map<String,Object> result(String source,UUID version,String hash,Map<String,Object> row,boolean reconciled,Object count){
    var result=new LinkedHashMap<String,Object>();result.put("status","PASS");result.put("sourceId",source);result.put("onboardingVersionId",version.toString());
    result.put("configurationHash",hash);result.put("attestationSubmitted",true);result.put("attestationId",String.valueOf(row.get("attestation_id")));
    result.put("reconciled",reconciled);if(count!=null)result.put("validatedRows",count);return result;
  }
  private static void requireRunning(){if(Thread.currentThread().isInterrupted())throw transientFailure("ING_READINESS_CANCELLED");}
  private static RuntimePorts.GatewayFailure transientFailure(String code){return new RuntimePorts.GatewayFailure(code,AdapterSpi.ErrorClass.TRANSIENT_SOURCE);}
}
