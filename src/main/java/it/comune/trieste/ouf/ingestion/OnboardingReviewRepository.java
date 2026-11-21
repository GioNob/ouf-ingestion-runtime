package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OnboardingReviewRepository {
  private final JdbcClient sql;
  public OnboardingReviewRepository(JdbcClient sql){this.sql=sql;}

  @Transactional
  public Optional<Claim> claim(String worker,Duration lease,boolean schemaOnly) {
    // A reclaimed reservation must reconcile its own transition before replay.
    sql.sql("""
        update ouf_ingestion.onboarding_review_outbox set state='FAILED_TERMINAL',
          last_error_code='ING_REVIEW_BUDGET_EXHAUSTED',lease_owner=null,lease_until=null
        where state in ('READY','FAILED_RETRYABLE','DELIVERING') and (not :schema or report_json is not null)
          and (lease_until is null or lease_until<=clock_timestamp())
          and (attempts>=128 or first_attempt_at+interval '24 hours'<=clock_timestamp()+interval '15 seconds')
        """).param("schema",schemaOnly).update();
    return sql.sql("""
        with candidate as (
          select request_id,post_reserved from ouf_ingestion.onboarding_review_outbox q
          where (not :schema or report_json is not null)
            and state in ('READY','FAILED_RETRYABLE','DELIVERING') and next_attempt_at<=clock_timestamp()
            and (lease_until is null or lease_until<=clock_timestamp()) and attempts<128
            and (first_attempt_at is null or first_attempt_at+interval '24 hours'>clock_timestamp()+interval '15 seconds')
            and not exists (select 1 from ouf_ingestion.onboarding_review_outbox earlier
              where earlier.issue_id=q.issue_id and earlier.notice_sequence<q.notice_sequence and earlier.state<>'ACKED')
          order by next_attempt_at,created_at,request_id for update of q skip locked limit 1)
        update ouf_ingestion.onboarding_review_outbox o set state='DELIVERING',attempts=attempts+1,
          post_reserved=true,first_attempt_at=coalesce(first_attempt_at,clock_timestamp()),lease_owner=:w,
          lease_until=least(clock_timestamp()+(:ms * interval '1 millisecond'),
            coalesce(first_attempt_at,clock_timestamp())+interval '24 hours')
        from candidate c,ouf_ingestion.runtime_issue i where o.request_id=c.request_id and i.issue_id=o.issue_id
        returning o.request_id,o.issue_id,i.source_id,i.evidence_ref,o.correlation_id,o.idempotency_key,o.attempts,c.post_reserved,o.notice_sequence is not null
        """).param("schema",schemaOnly).param("w",worker).param("ms",lease.toMillis())
        .query((r,n)->new Claim(r.getObject(1,UUID.class),r.getObject(2,UUID.class),r.getString(3),r.getString(4),
            r.getString(5),r.getString(6),worker,r.getInt(7),r.getBoolean(8),r.getBoolean(9))).optional();
  }
  public void requireCurrent(Claim claim) {
    if(sql.sql("""
        select 1 from ouf_ingestion.onboarding_review_outbox where request_id=:r and state='DELIVERING'
          and lease_owner=:w and attempts=:n and lease_until>clock_timestamp()+interval '12 seconds'
        """).param("r",claim.requestId()).param("w",claim.worker()).param("n",claim.attempt())
        .query(Integer.class).optional().isEmpty()) throw new IllegalStateException("ING_ONBOARDING_REVIEW_LEASE_LOST");
  }
  public String report(UUID issue,String key) {
    return sql.sql("""
        select (coalesce(opening.report_json,'{}'::jsonb) || q.report_json)::text
        from ouf_ingestion.onboarding_review_outbox q left join ouf_ingestion.onboarding_review_outbox opening
          on q.notice_sequence>1 and opening.issue_id=q.issue_id and opening.notice_sequence=1
        where q.issue_id=:i and q.idempotency_key=:k and q.report_json is not null
        """).param("i",issue).param("k",key).query(String.class).single();
  }

  public Optional<String> ownerRef(UUID issue) {
    return sql.sql("select onboarding_review_ref from ouf_ingestion.runtime_issue where issue_id=:i and onboarding_review_ref is not null")
        .param("i",issue).query(String.class).optional();
  }

  @Transactional public void acknowledge(Claim c,String ref) {
    int changed=sql.sql("""
        update ouf_ingestion.onboarding_review_outbox set state='ACKED',onboarding_review_ref=:ref,last_error_code=null,lease_owner=null,lease_until=null
        where request_id=:r and state='DELIVERING' and lease_owner=:w and attempts=:n and lease_until>clock_timestamp()
        """).param("ref",ref).param("r",c.requestId()).param("w",c.worker()).param("n",c.attempt()).update();
    if(changed!=1) throw new IllegalStateException("ING_ONBOARDING_REVIEW_LEASE_LOST");
    if (sql.sql("""
        update ouf_ingestion.runtime_issue set onboarding_review_ref=:ref,lock_version=lock_version+1
        where issue_id=:i and (not :notice or onboarding_review_ref is null or onboarding_review_ref=:ref)
        """).param("ref",ref).param("i",c.issueId()).param("notice",c.schemaNotice()).update()!=1)
      throw new IllegalStateException("ING_REVIEW_OWNER_ID_CHANGED");
  }

  @Transactional public void fail(Claim c,String code,boolean retry,Duration retryAfter) {
    long[] delays={5,15,60,300,900};
    long base=delays[Math.min(c.attempt()-1,delays.length-1)];
    long delay=base+Math.floorMod(Objects.hash(c.issueId(),c.attempt()),1+(int)base/5);
    if(retryAfter!=null) delay=Math.max(delay,retryAfter.toSeconds()+(retryAfter.getNano()==0?0:1));
    // A larger Retry-After cannot fit the incident budget.
    long bounded=Math.min(86401,delay);
    sql.sql("""
        update ouf_ingestion.onboarding_review_outbox set
          state=case when :retry and attempts<128 and clock_timestamp()+((:delay+15)*interval '1 second')<first_attempt_at+interval '24 hours'
            then 'FAILED_RETRYABLE' else 'FAILED_TERMINAL' end,
          last_error_code=:code,next_attempt_at=clock_timestamp()+(:delay*interval '1 second'),lease_owner=null,lease_until=null
        where request_id=:r and state='DELIVERING' and lease_owner=:w and attempts=:n and lease_until>clock_timestamp()
        """).param("retry",retry).param("delay",bounded).param("code",code)
        .param("r",c.requestId()).param("w",c.worker()).param("n",c.attempt()).update();
  }
  public record Claim(UUID requestId,UUID issueId,String sourceId,String evidenceRef,String correlationId,
                      String idempotencyKey,String worker,int attempt,boolean reconcile,boolean schemaNotice){}
}
