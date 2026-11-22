package it.comune.trieste.ouf.ingestion;
import java.util.*;import java.time.*;
class RunStateRepository {record ScheduleClaim(String sourceId,UUID publicationId,String publicationChecksum) {}}
class RunExecutionRepository {record Claim() {}}
class RunCoordinator {record Failure(String code){} static Failure classify(RuntimeException x){return new Failure(x.getMessage());}}
class DurablePipelineRepository {
 static Map<String,Object> handoff,lineage;
 record StageCommand(UUID run,String part,long seq,String obj,int attempt,String adapter,String version,String ref,String key,Map<String,Object> payload,Map<String,Object> evidence,Map<String,Object> ck,Map<String,Object> water,RunExecutionRepository.Claim c) {}
 Instant acquiredAt(Object c,long n,Instant i){return i;} Optional<String> beginLakeWrite(Object c,String suffix,String hash){return Optional.empty();}
 void requireRecoveryBudget(Object run,Object c){} void finishLakeWrite(Object c,String suffix,String hash,String ref){}
 UUID recordFailure(Object... args){throw new AssertionError(Arrays.toString(args));}
 void stage(StageCommand cmd,UUID attempt,UUID lin,UUID id){handoff=cmd.payload;lineage=cmd.evidence;}
}
class QuarantineService {UUID quarantine(Object... args){throw new AssertionError();}}
