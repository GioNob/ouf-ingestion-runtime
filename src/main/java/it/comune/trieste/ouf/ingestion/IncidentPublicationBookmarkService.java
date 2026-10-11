package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

/** Private owner coverage positions; never grants, delivery receipts or operator checkpoints. */
@Service
public class IncidentPublicationBookmarkService {
 public static final String PROFILE="ouf.owner-publication-bookmark.v1";
 private final JdbcClient sql;private final ObjectMapper json;private final IncidentSnapshotService snapshots;
 private final TransactionTemplate transaction;
 public IncidentPublicationBookmarkService(JdbcClient sql,ObjectMapper json,IncidentSnapshotService snapshots,PlatformTransactionManager manager){
  this.sql=sql;this.json=json;this.snapshots=snapshots;transaction=new TransactionTemplate(manager);transaction.setTimeout(2);
 }
 public Map<String,Object> start(String source,String state,Integer limit,TrustedAuthorizationContext.Context actor){
  requireActor(actor);
  var first=snapshots.publicationPage(source,state,limit,null,actor);
  return bind(first,null,null,actor);
 }
 public Map<String,Object> resume(UUID token,Integer limit,TrustedAuthorizationContext.Context actor){
  requireActor(actor);
  if(token==null||token.equals(new UUID(0,0)))throw failure(HttpStatus.BAD_REQUEST,"ING_BOOKMARK_INVALID");
  if(limit!=null&&(limit<1||limit>100))throw failure(HttpStatus.BAD_REQUEST,"ING_OPERATIONAL_QUERY_INVALID");
  var rows=sql.sql("""
   select b.*,b.expires_at<=clock_timestamp() as expired,
    coalesce((select revision from ouf_ingestion.incident_publication_history_floor where tenant_id=b.tenant_id),0)>b.high_watermark as history_gap
   from ouf_ingestion.incident_publication_bookmark b
   where b.token=:token and b.principal_id=:principal and b.tenant_id=:tenant and b.identity_digest=:identity
   """).param("token",token).param("principal",actor.subject()).param("tenant",actor.tenantId()).param("identity",identity(actor)).query().listOfRows();
  if(rows.isEmpty())throw failure(HttpStatus.NOT_FOUND,"ING_BOOKMARK_NOT_FOUND");
  var row=rows.getFirst();
  String policy=snapshots.requireCollection(actor,(String)row.get("source_id"));
  if(!policy.equals(row.get("policy_ref")))throw failure(HttpStatus.CONFLICT,"ING_BOOKMARK_POLICY_CHANGED");
  if(Boolean.TRUE.equals(row.get("expired"))||Boolean.TRUE.equals(row.get("history_gap")))
   throw failure(HttpStatus.GONE,"ING_BOOKMARK_HISTORY_GAP");
  var first=snapshots.publicationPage((String)row.get("source_id"),(String)row.get("state"),limit,((Number)row.get("high_watermark")).longValue(),actor);
  return bind(first,token,((Number)row.get("high_watermark")).longValue(),actor);
 }
 public Map<String,Object> page(UUID cursor,TrustedAuthorizationContext.Context actor){
  requireActor(actor);
  return decorate(snapshots.page(null,null,null,null,cursor,actor),actor);
 }
 private Map<String,Object> bind(Map<String,Object> first,UUID parent,Long parentWatermark,TrustedAuthorizationContext.Context actor){
  UUID session=UUID.fromString((String)first.get("snapshotRef"));
  transaction.executeWithoutResult(tx->sql.sql("insert into ouf_ingestion.incident_bookmark_scope(session_id,parent_token,parent_high_watermark,frame_digest) values(:session,:parent,:parentWatermark,:frame)")
   .param("session",session).param("parent",parent).param("parentWatermark",parentWatermark).param("frame",frameDigest(session)).update());
  return decorate(first,actor);
 }
 private Map<String,Object> decorate(Map<String,Object> body,TrustedAuthorizationContext.Context actor){
  UUID cursor=UUID.fromString((String)body.get("cursor"));
  return transaction.execute(tx->{
   var rows=sql.sql("""
    select s.*,c.position,b.parent_token,b.parent_high_watermark,b.frame_digest,s.expires_at<=clock_timestamp() as expired
    from ouf_ingestion.incident_read_cursor c join ouf_ingestion.incident_read_session s on s.session_id=c.session_id
    join ouf_ingestion.incident_bookmark_scope b on b.session_id=s.session_id
    where c.token=:cursor and s.principal_id=:principal and s.tenant_id=:tenant
    """).param("cursor",cursor).param("principal",actor.subject()).param("tenant",actor.tenantId()).query().listOfRows();
   if(rows.isEmpty())throw failure(HttpStatus.NOT_FOUND,"ING_BOOKMARK_SCOPE_NOT_FOUND");
   var frame=rows.getFirst();UUID session=(UUID)frame.get("session_id");
   if(Boolean.TRUE.equals(frame.get("expired")))throw failure(HttpStatus.GONE,"ING_BOOKMARK_SNAPSHOT_EXPIRED");
   if(!Objects.equals(frame.get("frame_digest"),frameDigest(session)))throw failure(HttpStatus.CONFLICT,"ING_BOOKMARK_CAPTURE_CHANGED");
   if(frame.get("since_at")!=null)throw failure(HttpStatus.CONFLICT,"ING_BOOKMARK_TIMESTAMP_UNPROVEN");
   boolean complete=!Boolean.TRUE.equals(body.get("partial"));
   sql.sql("""
    insert into ouf_ingestion.incident_bookmark_page(session_id,position,items_digest,complete) values(:id,:position,:digest,:complete)
    on conflict(session_id,position) do update set items_digest=excluded.items_digest,complete=excluded.complete
    """).param("id",session).param("position",frame.get("position")).param("digest",body.get("itemsDigest")).param("complete",complete).update();
   var result=new LinkedHashMap<String,Object>(body);result.put("bookmarkProfile",PROFILE);
   var coverage=new LinkedHashMap<String,Object>((Map<String,Object>)body.get("coverage"));
   coverage.put("bookmarkKind","READ_POSITION_NOT_OPERATOR_CONFIRMATION");result.put("coverage",coverage);
   if(complete&&!Boolean.TRUE.equals(body.get("truncated"))){
    int total=((Number)frame.get("item_count")).intValue(),pageLimit=((Number)frame.get("page_limit")).intValue();
    int expected=Math.max(1,(total+pageLimit-1)/pageLimit);
    int checked=sql.sql("select count(*)::integer from ouf_ingestion.incident_bookmark_page where session_id=:id and complete").param("id",session).query(Integer.class).single();
    if(checked==expected){
     if(!sql.sql("select pg_try_advisory_xact_lock(hashtextextended('ouf.incident.bookmark.capacity',0))").query(Boolean.class).single())
      throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_BOOKMARK_CAPACITY_UNAVAILABLE");
     // Recheck the page chain after acquiring issuance admission; no remote I/O.
     int fresh=sql.sql("select count(*)::integer from ouf_ingestion.incident_bookmark_page where session_id=:id and complete").param("id",session).query(Integer.class).single();
     if(fresh!=expected)throw failure(HttpStatus.CONFLICT,"ING_BOOKMARK_COVERAGE_INCOMPLETE");
     UUID issued=issue(frame,actor);result.put("publicationBookmark",issued.toString());
     coverage.put("noPublicationProgress",Objects.equals(frame.get("high_watermark"),frame.get("parent_high_watermark")));
     if(frame.get("parent_token")!=null)coverage.put("parentBookmark",frame.get("parent_token").toString());
    }
   }
   try{if(json.writeValueAsBytes(result).length>512<<10)throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_OPERATIONAL_RESULT_BUDGET_EXCEEDED");}
   catch(com.fasterxml.jackson.core.JsonProcessingException invalid){throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_BOOKMARK_EVIDENCE_INVALID");}
   return result;
  });
 }
 private UUID issue(Map<String,Object> frame,TrustedAuthorizationContext.Context actor){
  UUID session=(UUID)frame.get("session_id");
  var old=sql.sql("select token from ouf_ingestion.incident_publication_bookmark where session_id=:id").param("id",session).query(UUID.class).list();
  if(!old.isEmpty())return old.getFirst();
  var equivalent=sql.sql("""
   select token from ouf_ingestion.incident_publication_bookmark
   where principal_id=:principal and tenant_id=:tenant and identity_digest=:identity and policy_ref=:policy
     and source_id is not distinct from cast(:source as text) and state is not distinct from cast(:state as text)
     and high_watermark=:watermark and expires_at>clock_timestamp()
     and ((cast(:parent as uuid) is null and parent_token is null) or token=:parent)
   order by created_at,token limit 1
   """).param("principal",actor.subject()).param("tenant",actor.tenantId()).param("identity",identity(actor)).param("policy",frame.get("policy_ref"))
    .param("source",frame.get("source_id")).param("state",frame.get("state")).param("watermark",frame.get("high_watermark")).param("parent",frame.get("parent_token")).query(UUID.class).list();
  if(!equivalent.isEmpty())return equivalent.getFirst();
  sql.sql("delete from ouf_ingestion.incident_publication_bookmark where token in (select token from ouf_ingestion.incident_publication_bookmark where expires_at<=clock_timestamp() order by expires_at limit 1)").update();
  long global=sql.sql("select count(*) from ouf_ingestion.incident_publication_bookmark").query(Long.class).single();
  long tenant=sql.sql("select count(*) from ouf_ingestion.incident_publication_bookmark where tenant_id=:tenant").param("tenant",actor.tenantId()).query(Long.class).single();
  long principal=sql.sql("select count(*) from ouf_ingestion.incident_publication_bookmark where tenant_id=:tenant and principal_id=:principal").param("tenant",actor.tenantId()).param("principal",actor.subject()).query(Long.class).single();
  if(global>=32768||tenant>=4096||principal>=512)throw failure(HttpStatus.SERVICE_UNAVAILABLE,"ING_BOOKMARK_CAPACITY_UNAVAILABLE");
  UUID token=UUID.randomUUID();
  sql.sql("""
   insert into ouf_ingestion.incident_publication_bookmark(token,session_id,principal_id,tenant_id,identity_digest,policy_ref,source_id,state,high_watermark,parent_token)
   values(:token,:session,:principal,:tenant,:identity,:policy,:source,:state,:watermark,:parent)
   """).param("token",token).param("session",session).param("principal",actor.subject()).param("tenant",actor.tenantId()).param("identity",identity(actor))
    .param("policy",frame.get("policy_ref")).param("source",frame.get("source_id")).param("state",frame.get("state"))
    .param("watermark",frame.get("high_watermark")).param("parent",frame.get("parent_token")).update();
  return token;
 }
 private static void requireActor(TrustedAuthorizationContext.Context actor){
  if(actor==null||actor.owner()==null||!actor.owner().candidates().contains(IncidentSnapshotService.CAPABILITY))
   throw failure(HttpStatus.FORBIDDEN,"ING_OPERATIONAL_NOT_AUTHORIZED");
 }
 private String frameDigest(UUID session){
  String frame=sql.sql("select to_jsonb(s)::text from ouf_ingestion.incident_read_session s where session_id=:id").param("id",session).query(String.class).single();
  try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(frame.getBytes(java.nio.charset.StandardCharsets.UTF_8)));}
  catch(Exception invalid){throw failure(HttpStatus.CONFLICT,"ING_BOOKMARK_CAPTURE_CHANGED");}
 }
 private String identity(TrustedAuthorizationContext.Context actor){
  var p=actor.owner().principal();
  try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json.writeValueAsBytes(
   List.of(p.subjectId(),p.tenantId(),p.actorType().name(),Objects.toString(p.servicePrincipalId(),""),p.issuer(),p.audience()))));}
  catch(Exception invalid){throw failure(HttpStatus.FORBIDDEN,"ING_BOOKMARK_IDENTITY_UNPROVEN");}
 }
 private static ResponseStatusException failure(HttpStatus status,String code){return new ResponseStatusException(status,code);}
}
