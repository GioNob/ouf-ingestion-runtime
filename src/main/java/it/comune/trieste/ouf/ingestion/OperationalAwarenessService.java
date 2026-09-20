package it.comune.trieste.ouf.ingestion;

import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

/** Safe, bounded projection of persisted ingestion operational state for MCP/Gateway consumers. */
@Service
public class OperationalAwarenessService {
  private final JdbcClient sql;
  public OperationalAwarenessService(JdbcClient sql){this.sql=sql;}

  public List<Map<String,Object>> status(String source,int limit,TrustedAuthorizationContext.Context actor){
    return sql.sql("select h.source_id as source_ref,h.status as health_state,h.consecutive_failures,h.last_error_code as error_code,h.retry_not_before as next_retry_at,h.circuit_open_until,r.run_id as job_ref,r.state as run_state,r.updated_at as last_seen_at,r.correlation_id from ouf_ingestion.source_health h join lateral (select run_id,state,updated_at,correlation_id from ouf_ingestion.ing_run r where r.source_id=h.source_id and r.tenant_id=:t order by r.updated_at desc limit 1) r on true where (cast(:s as text) is null or h.source_id=:s) order by r.updated_at desc limit :n")
      .param("t",actor.tenantId()).param("s",source).param("n",bound(limit,100)).query().listOfRows();
  }

  public List<Map<String,Object>> history(String source,OffsetDateTime since,int limit,TrustedAuthorizationContext.Context actor){
    return sql.sql("select run_id as job_ref,source_id as source_ref,state as run_state,failure_code as error_code,correlation_id,created_at as first_seen_at,updated_at as last_seen_at from ouf_ingestion.ing_run where tenant_id=:t and (cast(:s as text) is null or source_id=:s) and (cast(:since as timestamptz) is null or updated_at>=:since) order by updated_at desc limit :n")
      .param("t",actor.tenantId()).param("s",source).param("since",since).param("n",bound(limit,200)).query().listOfRows();
  }

  public List<Map<String,Object>> incidents(String state,String source,OffsetDateTime since,int limit,TrustedAuthorizationContext.Context actor){
    if(state!=null&&!Set.of("OPEN","RECOVERING","RESOLVED").contains(state))throw new IllegalArgumentException("ING_OPERATIONAL_STATE_INVALID");
    return sql.sql("select i.issue_id as incident_id,'INGESTION' as module,i.issue_code as event_type,case when i.status='OPEN' then 'OPEN' else 'RESOLVED' end as lifecycle_state,case when i.severity='ERROR' then 'ERROR' else 'WARNING' end as severity,i.created_at as first_seen_at,coalesce(i.resolved_at,i.created_at) as last_seen_at,i.resolved_at,i.run_id as job_ref,i.source_id as source_ref,i.issue_code as error_code,r.correlation_id,case when i.status='OPEN' then true else false end as action_required,i.evidence_ref,concat('Ingestion ',lower(i.status),' issue ',i.issue_code) as impact_summary,concat(i.source_id,':',i.issue_code) as dedup_key,'TENANT_OPERATIONAL' as visibility_class from ouf_ingestion.runtime_issue i join ouf_ingestion.ing_run r on r.run_id=i.run_id where r.tenant_id=:t and (cast(:s as text) is null or i.source_id=:s) and (cast(:since as timestamptz) is null or i.created_at>=:since) and (cast(:st as text) is null or (case when i.status='OPEN' then 'OPEN' else 'RESOLVED' end)=:st) order by i.created_at desc limit :n")
      .param("t",actor.tenantId()).param("s",source).param("since",since).param("st",state).param("n",bound(limit,100)).query().listOfRows();
  }

  public Map<String,Object> summary(String source,OffsetDateTime since,int limit,TrustedAuthorizationContext.Context actor){
    // Catch-up rows and current health have different scopes. A small page or
    // recent since filter must not hide an older unresolved incident.
    var rows=incidents(null,source,since,limit,actor);
    var result=envelope("operations.status.read",rows,actor);
    var currentRows=incidents(null,source,null,100,actor);
    var current=envelope("operations.status.read",currentRows,actor);
    @SuppressWarnings("unchecked") var items=(List<Map<String,Object>>)current.get("items");
    long open=items.stream().filter(x->"OPEN".equals(x.get("lifecycle_state"))).count();
    boolean partial=Boolean.TRUE.equals(result.get("partial"))||Boolean.TRUE.equals(current.get("partial"))||currentRows.size()>=100||rows.size()>=bound(limit,100);
    var out=new LinkedHashMap<>(result);out.put("module","INGESTION");out.put("partial",partial);
    out.put("status",open>0?"DEGRADED":partial?"UNKNOWN":"HEALTHY");
    // Counts are only the authorized bounded scan, never advertised as totals.
    if(!partial)out.put("openIncidents",open);
    return out;
  }

  public Map<String,Object> envelope(String capability,List<Map<String,Object>> rows,TrustedAuthorizationContext.Context actor){
    var visible=new ArrayList<Map<String,Object>>();boolean partial=false;
    for(var row:rows){if(!visible(capability,row,actor)){partial=true;continue;}var safe=new LinkedHashMap<>(row);safe.remove("evidence_ref");safe.put("visibility_class","TENANT_OPERATIONAL");safe.put("authorization_decision_ref",actor.decisionRef()+":"+capability);visible.add(safe);}
    // An empty result must still be authorized for the requested operational collection.
    if(rows.isEmpty()&&!visible(capability,Map.of(),actor))partial=true;
    return Map.of("items",visible,"partial",partial,"authorization",partial?"REDACTED":"AUTHORIZED");
  }
  public void requireVisible(String capability,Map<String,Object> row,TrustedAuthorizationContext.Context actor){if(!visible(capability,row,actor))throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.FORBIDDEN,"ING_OPERATIONAL_NOT_AUTHORIZED");}
  private boolean visible(String capability,Map<String,Object> row,TrustedAuthorizationContext.Context actor){
    if(actor.owner()==null)return false;
    var attributes=new HashMap<String,String>();attributes.put("module","INGESTION");attributes.put("detailLevel","TENANT_OPERATIONAL");
    Object source=row.getOrDefault("source_ref",row.get("source_id")),job=row.getOrDefault("job_ref",row.get("run_id"));
    if(source!=null)attributes.put("sourceRef",source.toString());if(job!=null)attributes.put("jobRef",job.toString());
    Object id=row.getOrDefault("incident_id",row.getOrDefault("issue_id",job));
    var decision=actor.owner().decide(capability,new it.comune.trieste.ouf.authorization.ResourceContext("operational",id==null?null:id.toString(),actor.tenantId(),null,attributes));
    return decision.allowed()&&"TENANT_OPERATIONAL".equals(decision.permittedDetailLevel());
  }

  private static int bound(int value,int max){return Math.max(1,Math.min(value,max));}
}
