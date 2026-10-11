package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.*;
import it.comune.trieste.ouf.authorization.ResourceContext;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

/** Owner-local stable pages. Tokens are metadata, never authority or read receipts. */
@Service
public class IncidentSnapshotService {
  public static final String CAPABILITY="operations.incident.read",PROFILE="ouf.incident-owner-cursor.v1";
  private static final int MAX_ITEMS=20000,MAX_PAGES=200,MAX_GLOBAL_ITEMS=200000,MAX_TENANT_ITEMS=64000;
  private final JdbcClient sql;private final ObjectMapper json;private final OperationalAwarenessService projection;
  private final TransactionTemplate transaction;
  public IncidentSnapshotService(JdbcClient sql,ObjectMapper json,OperationalAwarenessService projection,PlatformTransactionManager manager){
    this.sql=sql;this.json=json;this.projection=projection;this.transaction=new TransactionTemplate(manager);this.transaction.setTimeout(2);
  }

  /** Bounded maintenance only; publication does not renew grants or change incident truth. */
  public int publish(String tenant,String source){
    return transaction.execute(tx->sql.sql("select ouf_ingestion.publish_incident_transitions(:tenant,:source,256)").param("tenant",tenant).param("source",source).query(Integer.class).single());
  }

  public Map<String,Object> page(String source,String state,OffsetDateTime since,Integer requested,UUID cursor,TrustedAuthorizationContext.Context actor){
    if(actor.owner()==null||!actor.owner().candidates().contains(CAPABILITY))throw failure(HttpStatus.FORBIDDEN,"ING_OPERATIONAL_NOT_AUTHORIZED");
    if(cursor!=null){
      if(source!=null||state!=null||since!=null||requested!=null)throw failure(HttpStatus.BAD_REQUEST,"ING_CURSOR_FILTERS_IMMUTABLE");
      return transaction.execute(tx->read(cursor,actor));
    }
    int limit=requested==null?50:requested;
    if(limit<1||limit>100||(source!=null&&(source.isBlank()||source.codePointCount(0,source.length())>200))||
       (state!=null&&!Set.of("OPEN","RECOVERING","RESOLVED").contains(state)))throw failure(HttpStatus.BAD_REQUEST,"ING_OPERATIONAL_QUERY_INVALID");
    String policy=requireCollection(actor,source);
    // At most four local batches, no remote I/O or retry loop. Remaining lag is explicit.
    return transaction.execute(tx->{
      for(int batch=0;batch<4;batch++)if(publish(actor.tenantId(),source)<256)break;
      UUID first=create(source,state,since,limit,policy,actor);
      return read(first,actor);
    });
  }

  private UUID create(String source,String state,OffsetDateTime since,int limit,String policy,TrustedAuthorizationContext.Context actor){
    boolean locked=sql.sql("select pg_try_advisory_xact_lock(hashtextextended('ouf.incident.cursor.capacity',0))").query(Boolean.class).single();
    if(!locked)throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_CAPACITY_UNAVAILABLE");
    sql.sql("delete from ouf_ingestion.incident_read_session where session_id in (select session_id from ouf_ingestion.incident_read_session where expires_at<=clock_timestamp() order by expires_at limit 1)").update();
    long globalSessions=count("select count(*) from ouf_ingestion.incident_read_session");
    long tenantSessions=sql.sql("select count(*) from ouf_ingestion.incident_read_session where tenant_id=:t").param("t",actor.tenantId()).query(Long.class).single();
    long principalSessions=sql.sql("select count(*) from ouf_ingestion.incident_read_session where tenant_id=:t and principal_id=:p").param("t",actor.tenantId()).param("p",actor.subject()).query(Long.class).single();
    if(globalSessions>=4096||tenantSessions>=128||principalSessions>=16)throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_CAPACITY_UNAVAILABLE");
    long globalItems=count("select count(*) from ouf_ingestion.incident_read_item");
    long tenantItems=sql.sql("select count(*) from ouf_ingestion.incident_read_item i join ouf_ingestion.incident_read_session s on s.session_id=i.session_id where s.tenant_id=:t").param("t",actor.tenantId()).query(Long.class).single();
    int available=(int)Math.min(Math.min(MAX_ITEMS,limit*MAX_PAGES),Math.min(MAX_GLOBAL_ITEMS-globalItems,MAX_TENANT_ITEMS-tenantItems));
    if(available<0)throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_CAPACITY_UNAVAILABLE");
    long watermark=sql.sql("select coalesce((select revision from ouf_ingestion.incident_publication_clock where tenant_id=:t),0)").param("t",actor.tenantId()).query(Long.class).single();
    boolean lag=sql.sql("select exists(select 1 from ouf_ingestion.incident_transition t join ouf_ingestion.runtime_issue i on i.issue_id=t.issue_id join ouf_ingestion.ing_run r on r.run_id=i.run_id where t.tenant_id=:tenant and r.tenant_id=:tenant and t.publication_revision is null and (cast(:source as text) is null or t.source_id=:source))").param("tenant",actor.tenantId()).param("source",source).query(Boolean.class).single();
    boolean gap=since!=null&&sql.sql("select :since<baseline_at from ouf_ingestion.incident_history_coverage where singleton").param("since",since).query(Boolean.class).single();
    UUID session=UUID.randomUUID();
    sql.sql("insert into ouf_ingestion.incident_read_session(session_id,principal_id,tenant_id,actor_type,service_principal_id,policy_ref,principal_fingerprint,source_id,state,since_at,page_limit,high_watermark,publication_lag,history_gap) values(:id,:principal,:tenant,:actor,:service,:policy,:fingerprint,:source,:state,:since,:limit,:watermark,:lag,:gap)")
      .param("id",session).param("principal",actor.subject()).param("tenant",actor.tenantId()).param("actor",actor.actorType()).param("service",servicePrincipal(actor)).param("policy",policy).param("fingerprint",fingerprint(actor)).param("source",source).param("state",state).param("since",since).param("limit",limit).param("watermark",watermark).param("lag",lag).param("gap",gap).update();
    int selected=sql.sql("""
      with latest as (
        select distinct on (t.issue_id) t.issue_id,t.body,t.observed_at,t.source_id
        from ouf_ingestion.incident_transition t
        where t.tenant_id=:tenant and t.publication_revision<=:watermark and (cast(:source as text) is null or t.source_id=:source)
        order by t.issue_id,t.publication_revision desc
      ), ranked as (
        select l.*,row_number() over(order by
          case when l.body->>'lifecycle_state'='OPEN' then 0 else 1 end,
          case when l.body->>'severity'='ERROR' then 2 else 1 end desc,
          l.observed_at desc,l.issue_id asc)-1 as position
        from latest l
        where (cast(:source as text) is null or l.source_id=:source)
          and (cast(:state as text) is null or l.body->>'lifecycle_state'=:state)
          and (cast(:since as timestamptz) is null or l.observed_at>=:since)
          and exists(select 1 from ouf_ingestion.runtime_issue i join ouf_ingestion.ing_run r on r.run_id=i.run_id where i.issue_id=l.issue_id and r.tenant_id=:tenant)
        order by position limit :scan
      ), tally as (select count(*)::integer as n from ranked),
      inserted as (
        insert into ouf_ingestion.incident_read_item(session_id,position,body)
        select :id,position::integer,body from ranked where (select n from tally)<=:available returning position
      ) select n from tally
      """).param("tenant",actor.tenantId()).param("watermark",watermark).param("source",source).param("state",state).param("since",since).param("scan",available+1).param("available",available).param("id",session).query(Integer.class).single();
    if(selected>available)throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_CAPACITY_UNAVAILABLE");
    sql.sql("update ouf_ingestion.incident_read_session set item_count=:count where session_id=:id").param("count",selected).param("id",session).update();
    return token(session,0);
  }

  private Map<String,Object> read(UUID cursor,TrustedAuthorizationContext.Context actor){
    var sessions=sql.sql("""
      select s.*,c.position,
        s.expires_at<=clock_timestamp() as expired,
        to_char(s.created_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as snapshot_as_of,
        to_char(s.expires_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as snapshot_expires_at
      from ouf_ingestion.incident_read_cursor c join ouf_ingestion.incident_read_session s on s.session_id=c.session_id
      where c.token=:token and s.principal_id=:principal and s.tenant_id=:tenant and s.actor_type=:actor
        and s.service_principal_id=:service and s.principal_fingerprint=:fingerprint
      """).param("token",cursor).param("principal",actor.subject()).param("tenant",actor.tenantId()).param("actor",actor.actorType()).param("service",servicePrincipal(actor)).param("fingerprint",fingerprint(actor)).query().listOfRows();
    if(sessions.isEmpty())throw failure(HttpStatus.NOT_FOUND,"ING_CURSOR_UNAVAILABLE");
    var session=sessions.getFirst();
    String policy=requireCollection(actor,(String)session.get("source_id"));
    if(!policy.equals(session.get("policy_ref")))throw failure(HttpStatus.CONFLICT,"ING_CURSOR_AUTHORIZATION_CHANGED");
    if(Boolean.TRUE.equals(session.get("expired")))throw failure(HttpStatus.GONE,"ING_CURSOR_EXPIRED");
    int position=number(session.get("position")),limit=number(session.get("page_limit")),total=number(session.get("item_count"));
    UUID id=(UUID)session.get("session_id");
    var stored=sql.sql("""
      select p.body::text as body from ouf_ingestion.incident_read_item p
      where p.session_id=:id and p.position>=:start and p.position<:end
        and exists(select 1 from ouf_ingestion.runtime_issue i join ouf_ingestion.ing_run r on r.run_id=i.run_id
          where i.issue_id=cast(p.body->>'incident_id' as uuid) and r.tenant_id=:tenant)
      order by p.position limit :limit
      """).param("id",id).param("start",position).param("end",position+limit).param("tenant",actor.tenantId()).param("limit",limit).query().listOfRows();
    var rows=new ArrayList<Map<String,Object>>();
    try{for(var row:stored){@SuppressWarnings("unchecked") var item=json.readValue((String)row.get("body"),LinkedHashMap.class);rows.add(item);}}
    catch(Exception invalid){throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_EVIDENCE_INVALID");}
    var result=new LinkedHashMap<String,Object>(rows.isEmpty()?Map.of("items",List.of(),"partial",false,"authorization","AUTHORIZED"):projection.envelope(CAPABILITY,rows,actor));
    boolean hidden=stored.size()!=Math.max(0,Math.min(limit,total-position));
    boolean lag=Boolean.TRUE.equals(session.get("publication_lag")),gap=Boolean.TRUE.equals(session.get("history_gap"));
    result.put("partial",Boolean.TRUE.equals(result.get("partial"))||hidden||lag||gap);
    if(hidden)result.put("authorization","REDACTED");
    result.put("module","INGESTION");result.put("cursor",cursor.toString());
    int next=position+limit;
    if(next<total)result.put("nextCursor",token(id,next).toString());
    result.put("truncated",next<total);result.put("snapshotRef",id.toString());
    result.put("snapshotAsOf",session.get("snapshot_as_of"));result.put("snapshotExpiresAt",session.get("snapshot_expires_at"));
    result.put("priorityProfile","ouf.incident-priority.v1");result.put("cursorProfile",PROFILE);
    result.put("coverage",Map.of("kind","PUBLISHED_OWNER_SNAPSHOT","publicationLag",lag,"historyGap",gap));
    result.put("consultationRecorded",false);
    try{result.put("itemsDigest",HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json.writeValueAsBytes(result.get("items")))));}
    catch(Exception invalid){throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_EVIDENCE_INVALID");}
    try{if(json.writeValueAsBytes(result).length>512<<10)throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_OPERATIONAL_RESULT_BUDGET_EXCEEDED");}
    catch(com.fasterxml.jackson.core.JsonProcessingException invalid){throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_CURSOR_EVIDENCE_INVALID");}
    return result;
  }

  private String requireCollection(TrustedAuthorizationContext.Context actor,String source){
    var attributes=new HashMap<String,String>();attributes.put("module","INGESTION");attributes.put("detailLevel","TENANT_OPERATIONAL");if(source!=null)attributes.put("sourceRef",source);
    var decision=actor.owner().decide(CAPABILITY,new ResourceContext("operational",null,actor.tenantId(),null,attributes));
    if(!decision.allowed()||!"TENANT_OPERATIONAL".equals(decision.permittedDetailLevel()))throw failure(HttpStatus.FORBIDDEN,"ING_OPERATIONAL_NOT_AUTHORIZED");
    return decision.decisionRef();
  }
  private UUID token(UUID session,int position){
    UUID proposed=UUID.randomUUID();
    sql.sql("insert into ouf_ingestion.incident_read_cursor(token,session_id,position) values(:token,:id,:position) on conflict(session_id,position) do nothing").param("token",proposed).param("id",session).param("position",position).update();
    return sql.sql("select token from ouf_ingestion.incident_read_cursor where session_id=:id and position=:position").param("id",session).param("position",position).query(UUID.class).single();
  }
  private long count(String query){return sql.sql(query).query(Long.class).single();}
  private static int number(Object value){return ((Number)value).intValue();}
  private static String servicePrincipal(TrustedAuthorizationContext.Context actor){return Objects.toString(actor.owner().principal().servicePrincipalId(),"");}
  private String fingerprint(TrustedAuthorizationContext.Context actor){
    try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical(json.valueToTree(actor.owner().principal())).getBytes(StandardCharsets.UTF_8)));}
    catch(Exception invalid){throw failure(HttpStatus.FORBIDDEN,"ING_OPERATIONAL_CONTEXT_INVALID");}
  }
  private String canonical(JsonNode node)throws com.fasterxml.jackson.core.JsonProcessingException {
    if(node.isObject()){var fields=new TreeMap<String,String>();var iterator=node.fields();while(iterator.hasNext()){var field=iterator.next();fields.put(field.getKey(),canonical(field.getValue()));}var values=new ArrayList<String>();for(var field:fields.entrySet())values.add(json.writeValueAsString(field.getKey())+":"+field.getValue());return "{"+String.join(",",values)+"}";}
    if(node.isArray()){var values=new ArrayList<String>();for(var value:node)values.add(canonical(value));Collections.sort(values);return "["+String.join(",",values)+"]";}
    return node.toString();
  }
  private static ResponseStatusException failure(HttpStatus status,String code){return new ResponseStatusException(status,code);}
}
