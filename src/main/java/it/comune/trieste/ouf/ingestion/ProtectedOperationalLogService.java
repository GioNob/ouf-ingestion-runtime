package it.comune.trieste.ouf.ingestion;

import jakarta.servlet.http.HttpServletRequest;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProtectedOperationalLogService {
  private final JdbcClient sql;public ProtectedOperationalLogService(JdbcClient sql){this.sql=sql;}
  @Transactional public Map<String,Object> search(Search q,TrustedAuthorizationContext.Context actor,String requestCorrelation){validate(q);audit("SEARCH","ALLOWED",actor,q.purpose(),requestCorrelation);List<Map<String,Object>> items=sql.sql("select event_id,occurred_at,severity,event_type,correlation_id,run_id,source_id,resource_type,resource_id,error_code,detail_json from ouf_ingestion.operational_event where occurred_at>=:f and occurred_at<:t and (:severity is null or severity=:severity) and (:correlation is null or correlation_id=:correlation) and (:run is null or run_id=:run) and (:source is null or source_id=:source) order by occurred_at desc,event_id desc limit :n").param("f",q.from()).param("t",q.to()).param("severity",q.severity()).param("correlation",q.correlationId()).param("run",q.runId()).param("source",q.sourceId()).param("n",limit(q.limit())).query().listOfRows();return Map.of("items",items,"truncated",items.size()==limit(q.limit()));}
  @Transactional public List<Map<String,Object>> aggregate(Search q,String dimension,TrustedAuthorizationContext.Context actor,String requestCorrelation){validate(q);if(!Set.of("severity","event_type","source_id").contains(dimension))throw new IllegalArgumentException("ING_LOG_DIMENSION_INVALID");audit("AGGREGATE","ALLOWED",actor,q.purpose(),requestCorrelation);String column=dimension;return sql.sql("select "+column+" as key,count(*) as count from ouf_ingestion.operational_event where occurred_at>=:f and occurred_at<:t and (:correlation is null or correlation_id=:correlation) group by "+column+" order by count(*) desc,"+column+" limit :n").param("f",q.from()).param("t",q.to()).param("correlation",q.correlationId()).param("n",limit(q.limit())).query().listOfRows();}
  @Transactional public void denied(HttpServletRequest request,String operation,String purpose,String correlation){String subject=request.getUserPrincipal()==null?"anonymous":request.getUserPrincipal().getName();auditRaw(operation,"DENIED",subject,attribute(request,TrustedAuthorizationContext.ACTOR_TYPE,"UNKNOWN"),attribute(request,TrustedAuthorizationContext.TENANT,"UNKNOWN"),attribute(request,TrustedAuthorizationContext.DECISION,"UNAVAILABLE"),purpose,correlation);}
  private void audit(String operation,String outcome,TrustedAuthorizationContext.Context a,String purpose,String correlation){auditRaw(operation,outcome,a.subject(),a.actorType(),a.tenantId(),a.decisionRef(),purpose,correlation);}
  private void auditRaw(String op,String outcome,String subject,String type,String tenant,String decision,String purpose,String correlation){sql.sql("insert into ouf_ingestion.protected_log_access_audit(access_id,operation,outcome,actor_subject,actor_type,tenant_id,authorization_decision_ref,purpose,request_correlation_id) values(:i,:o,:x,:s,:t,:n,:d,:p,:c)").param("i",UUID.randomUUID()).param("o",op).param("x",outcome).param("s",subject).param("t",type).param("n",tenant).param("d",decision).param("p",purpose).param("c",correlation).update();}
  private static void validate(Search q){if(q.from()==null||q.to()==null||!q.from().isBefore(q.to())||Duration.between(q.from(),q.to()).compareTo(Duration.ofHours(24))>0)throw new IllegalArgumentException("ING_LOG_WINDOW_INVALID");if(q.purpose()==null||q.purpose().isBlank()||q.purpose().length()>500)throw new IllegalArgumentException("ING_LOG_PURPOSE_REQUIRED");if(q.severity()!=null&&!Set.of("INFO","WARN","ERROR").contains(q.severity()))throw new IllegalArgumentException("ING_LOG_SEVERITY_INVALID");}
  private static int limit(Integer n){return Math.max(1,Math.min(n==null?100:n,200));}
  private static String attribute(HttpServletRequest r,String name,String fallback){Object v=r.getAttribute(name);return v instanceof String s&&!s.isBlank()?s:fallback;}
  public record Search(OffsetDateTime from,OffsetDateTime to,String severity,String correlationId,UUID runId,String sourceId,Integer limit,String purpose){}
}
