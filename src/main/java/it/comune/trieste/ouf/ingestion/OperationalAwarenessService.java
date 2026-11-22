package it.comune.trieste.ouf.ingestion;

import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

/** Safe, bounded projection of persisted ingestion operational state for MCP/Gateway consumers. */
@Service
public class OperationalAwarenessService {
  private final JdbcClient sql;
  private final OperationalRecovery recovery;
  public OperationalAwarenessService(JdbcClient sql){this(sql,"");}
  @Autowired
  public OperationalAwarenessService(JdbcClient sql,@Value("${ouf.ingestion.recovery.ths-origin:}") String thsOrigin){
    this.sql=sql;
    this.recovery=new OperationalRecovery(thsOrigin);
  }

  public List<Map<String,Object>> status(String source,int limit,TrustedAuthorizationContext.Context actor){
    return sql.sql("""
      with page as materialized (
        select tenant_id,source_ref,publication_id,schedule_state,health_state,consecutive_failures,error_code,
               next_retry_at,circuit_open_until,job_ref,control_version,run_state,last_seen_at,correlation_id,
               true project_schedule
        from ouf_ingestion.current_source_status
        where tenant_id=:t and (cast(:s as text) is null or source_ref=:s)
        order by last_seen_at desc nulls last,source_ref limit :n
      )
      """+OperationalRecovery.SELECT+" order by page.last_seen_at desc nulls last,page.source_ref")
      .param("t",actor.tenantId()).param("s",source).param("n",bound(limit,100)).query().listOfRows()
      .stream().map(recovery::project).toList();
  }

  public List<Map<String,Object>> history(String source,OffsetDateTime since,int limit,TrustedAuthorizationContext.Context actor){
    return sql.sql("""
      with page as materialized (
        select history.*, is_current_publication project_schedule from (
          select r.tenant_id,r.run_id job_ref,r.source_id source_ref,r.publication_id,r.state run_state,r.failure_code error_code,
            r.correlation_id,r.created_at first_seen_at,r.updated_at last_seen_at,
            r.publication_id is not null and r.publication_id is not distinct from s.publication_id is_current_publication
          from ouf_ingestion.ing_run r left join ouf_ingestion.ing_schedule s
            on s.tenant_id=r.tenant_id and s.source_id=r.source_id
          where r.tenant_id=:t and (cast(:s as text) is null or r.source_id=:s)
          union all
          select tenant_id,job_ref,source_ref,publication_id,run_state,error_code,correlation_id,null,last_seen_at,true
          from ouf_ingestion.current_source_status where tenant_id=:t and job_ref is null
            and publication_id is not null and (cast(:s as text) is null or source_ref=:s)
        ) history where (cast(:since as timestamptz) is null or last_seen_at>=:since)
        order by is_current_publication desc,last_seen_at desc,source_ref,job_ref desc limit :n
      )
      """+OperationalRecovery.SELECT+" order by page.is_current_publication desc,page.last_seen_at desc,page.source_ref,page.job_ref desc")
      .param("t",actor.tenantId()).param("s",source).param("since",since).param("n",bound(limit,200)).query().listOfRows()
      .stream().map(recovery::project).toList();
  }

  public List<Map<String,Object>> incidents(String state,String source,OffsetDateTime since,int limit,TrustedAuthorizationContext.Context actor){
    if(state!=null&&!Set.of("OPEN","RECOVERING","RESOLVED").contains(state))throw new IllegalArgumentException("ING_OPERATIONAL_STATE_INVALID");
    return sql.sql("select i.issue_id as incident_id,'INGESTION' as module,i.issue_code as event_type,case when i.status='OPEN' then 'OPEN' else 'RESOLVED' end as lifecycle_state,case when i.severity='ERROR' then 'ERROR' else 'WARNING' end as severity,i.created_at as first_seen_at,i.last_updated_at as last_seen_at,i.resolved_at,i.run_id as job_ref,i.source_id as source_ref,i.issue_code as error_code,r.correlation_id,case when i.status='OPEN' then true else false end as action_required,i.evidence_ref,concat('Ingestion ',lower(i.status),' issue ',i.issue_code) as impact_summary,concat(i.source_id,':',i.issue_code) as dedup_key,'TENANT_OPERATIONAL' as visibility_class from ouf_ingestion.runtime_issue i join ouf_ingestion.ing_run r on r.run_id=i.run_id where r.tenant_id=:t and (cast(:s as text) is null or i.source_id=:s) and (cast(:since as timestamptz) is null or i.last_updated_at>=:since) and (cast(:st as text) is null or (case when i.status='OPEN' then 'OPEN' else 'RESOLVED' end)=:st) order by case when i.status='OPEN' then 0 else 1 end,case when i.severity='ERROR' then 2 else 1 end desc,i.last_updated_at desc,i.issue_id asc limit :n")
      .param("t",actor.tenantId()).param("s",source).param("since",since).param("st",state).param("n",bound(limit,100)).query().listOfRows();
  }

  public Map<String,Object> summary(String source,OffsetDateTime since,int limit,TrustedAuthorizationContext.Context actor){
    var result=envelope("operations.status.read",incidents(null,source,since,limit,actor),actor);
    @SuppressWarnings("unchecked") var items=(List<Map<String,Object>>)result.get("items");long open=items.stream().filter(x->"OPEN".equals(x.get("lifecycle_state"))).count();
    var out=new LinkedHashMap<>(result);out.put("module","INGESTION");boolean partial=Boolean.TRUE.equals(out.get("partial"));boolean degraded=open>0;
    if(!partial && !degraded){
      var unhealthy=sql.sql("""
        select source_ref,job_ref from ouf_ingestion.current_source_status
        where tenant_id=:t and (cast(:s as text) is null or source_ref=:s)
          and (health_state in ('DEGRADED','PAUSED') or run_state in ('RETRY_WAIT','BLOCKED','PAUSED'))
        order by source_ref limit 1
        """).param("t",actor.tenantId()).param("s",source).query().listOfRows();
      if(!unhealthy.isEmpty()){
        degraded=visible("operations.status.read",unhealthy.getFirst(),actor);
        if(!degraded){partial=true;out.put("partial",true);out.put("authorization","REDACTED");}
      }
    }
    out.put("status",partial?"UNKNOWN":degraded?"DEGRADED":"HEALTHY");out.put("openIncidents",open);return out;
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
