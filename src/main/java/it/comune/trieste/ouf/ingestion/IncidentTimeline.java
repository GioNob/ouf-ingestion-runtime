package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Snapshot/keyset pagination over persisted owner events, with authorization on every returned row. */
@Service
public class IncidentTimeline {
  private final JdbcClient sql;
  private final ObjectMapper json;
  private final OperationalAwarenessService visibility;
  public IncidentTimeline(JdbcClient sql,ObjectMapper json,OperationalAwarenessService visibility){this.sql=sql;this.json=json;this.visibility=visibility;}
  public record Query(String sourceId,String state,OffsetDateTime since,Integer limit,OffsetDateTime until,String cursor,UUID jobId,String severity){}
  private record Cursor(long snapshot,long before,OffsetDateTime since,OffsetDateTime until,OffsetDateTime expires,String binding){}

  public Map<String,Object> page(Query q,TrustedAuthorizationContext.Context actor){
    if(q==null)q=new Query(null,null,null,null,null,null,null,null);
    int limit=q.limit()==null?50:q.limit();
    if(limit<1||limit>100||(q.sourceId()!=null&&(q.sourceId().isBlank()||q.sourceId().length()>200))||
       (q.state()!=null&&!Set.of("OPEN","RECOVERING","RESOLVED").contains(q.state()))||
       (q.severity()!=null&&!Set.of("INFO","WARNING","ERROR","CRITICAL").contains(q.severity())))throw invalid();
    var now=OffsetDateTime.now(ZoneOffset.UTC);
    String binding=digest(actor.tenantId()+"\n"+actor.subject()+"\n"+q.sourceId()+"\n"+q.state()+"\n"+q.jobId()+"\n"+q.severity()+"\n"+limit);
    Cursor c;
    if(q.cursor()!=null){
      try {if(q.cursor().length()>4096)throw invalid();c=json.readValue(Base64.getUrlDecoder().decode(q.cursor()),Cursor.class);}catch(Exception e){throw invalid();}
      if(c==null||!binding.equals(c.binding())||c.expires()==null||!c.expires().isAfter(now)||c.expires().isAfter(now.plusMinutes(16))||c.snapshot()<0||c.before()<1||
         (q.since()!=null&&!q.since().isEqual(c.since()))||(q.until()!=null&&!q.until().isEqual(c.until())))throw invalid();
    }else{
      var until=q.until()==null?now:q.until();var since=q.since()==null?until.minusDays(1):q.since();
      long snapshot=sql.sql("select coalesce(max(sequence_id),0) from ouf_ingestion.operational_incident_transition").query(Long.class).single();
      c=new Cursor(snapshot,Long.MAX_VALUE,since,until,now.plusMinutes(15),binding);
    }
    if(c.since()==null||c.until()==null||c.since().isAfter(c.until())||c.until().isAfter(now.plusSeconds(1))||Duration.between(c.since(),c.until()).compareTo(Duration.ofDays(30))>0)throw invalid();
    var rows=sql.sql("""
      with snapshot as (select distinct on(incident_id) sequence_id,projection from ouf_ingestion.operational_incident_transition
       where tenant_id=:tenant and sequence_id<=:snapshot and occurred_at<=:until order by incident_id,sequence_id desc)
      select sequence_id,projection::text from snapshot where sequence_id<:before
       and (projection->>'last_seen_at')::timestamptz between :since and :until
       and (cast(:source as text) is null or projection->>'source_ref'=:source)
       and (cast(:state as text) is null or projection->>'lifecycle_state'=:state)
       and (cast(:job as text) is null or projection->>'job_ref'=:job)
       and (cast(:severity as text) is null or projection->>'severity'=:severity)
       order by sequence_id desc limit :limit
      """).param("tenant",actor.tenantId()).param("snapshot",c.snapshot()).param("before",c.before()).param("since",c.since()).param("until",c.until())
      .param("source",q.sourceId()).param("state",q.state()).param("job",q.jobId()==null?null:q.jobId().toString()).param("severity",q.severity()).param("limit",limit+1).query().listOfRows();
    boolean more=rows.size()>limit;var selected=rows.subList(0,Math.min(limit,rows.size()));
    var projections=selected.stream().map(row->decode((String)row.get("projection"))).toList();
    var out=new LinkedHashMap<>(visibility.envelope("operations.incident.read",projections,actor));
    out.put("since",c.since());out.put("until",c.until());out.put("hasMore",more);
    if(more){long before=((Number)selected.getLast().get("sequence_id")).longValue();
      try{out.put("nextCursor",Base64.getUrlEncoder().withoutPadding().encodeToString(json.writeValueAsBytes(new Cursor(c.snapshot(),before,c.since(),c.until(),c.expires(),binding))));}catch(Exception e){throw new IllegalStateException(e);}}
    return out;
  }

  public Optional<Map<String,Object>> explain(UUID id,TrustedAuthorizationContext.Context actor){
    var current=sql.sql("select t.projection::text from ouf_ingestion.operational_incident_transition t where t.incident_id=:id and t.tenant_id=:tenant order by sequence_id desc limit 1")
      .param("id",id).param("tenant",actor.tenantId()).query(String.class).optional();
    if(current.isEmpty())return Optional.empty();
    var row=decode(current.orElseThrow());visibility.requireVisible("operations.incident.explain",row,actor);
    var events=sql.sql("select projection::text from ouf_ingestion.operational_incident_transition where incident_id=:id and tenant_id=:tenant order by sequence_id desc limit 201")
      .param("id",id).param("tenant",actor.tenantId()).query(String.class).list();
    var out=new LinkedHashMap<>(row);out.put("transitions",events.stream().limit(200).map(this::decode).toList());out.put("partial",events.size()>200);
    out.put("authorization_decision_ref",actor.decisionRef()+":operations.incident.explain");return Optional.of(out);
  }
  @SuppressWarnings("unchecked") private Map<String,Object> decode(String raw){try{return json.readValue(raw,Map.class);}catch(Exception e){throw new IllegalStateException("ING_INCIDENT_PROJECTION_INVALID",e);}}
  private static String digest(String value){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}}
  private static ResponseStatusException invalid(){return new ResponseStatusException(HttpStatus.BAD_REQUEST,"ING_INCIDENT_QUERY_INVALID");}
}
