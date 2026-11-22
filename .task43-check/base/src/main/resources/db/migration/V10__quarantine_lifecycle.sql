alter table ouf_ingestion.ing_quarantine
  add column lifecycle_state text,
  add column lifecycle_version bigint not null default 0,
  add column resolution_reason text,
  add column resolution_actor text,
  add column authorization_decision_ref text,
  add column superseded_by text;

update ouf_ingestion.ing_quarantine set lifecycle_state=case state
  when 'OPEN' then 'OPEN'
  when 'REPLAY_REQUESTED' then 'REPROCESSING'
  when 'RELEASED' then 'RESOLVED'
  when 'REJECTED' then 'DISMISSED'
end;

alter table ouf_ingestion.ing_quarantine
  alter column lifecycle_state set default 'OPEN',
  alter column lifecycle_state set not null,
  add constraint ck_ing_quarantine_lifecycle_state check
    (lifecycle_state in ('OPEN','RETRY_READY','REPROCESSING','RESOLVED','DISMISSED','SUPERSEDED')),
  add constraint ck_ing_quarantine_resolution_reason check
    (resolution_reason is null or length(resolution_reason) between 1 and 1000);

create index ix_ing_quarantine_lifecycle on ouf_ingestion.ing_quarantine(lifecycle_state,created_at desc);

create or replace function ouf_ingestion.capture_quarantine_event() returns trigger language plpgsql as $$
declare r ouf_ingestion.ing_run%rowtype;
begin
  select * into r from ouf_ingestion.ing_run where run_id=new.run_id;
  insert into ouf_ingestion.operational_event(event_type,severity,run_id,source_id,resource_type,resource_id,error_code,correlation_id,detail_json)
  values('QUARANTINE_'||new.lifecycle_state,
    case when new.lifecycle_state in ('OPEN','REPROCESSING') then 'WARN' else 'INFO' end,
    new.run_id,r.source_id,'QUARANTINE',new.quarantine_id::text,new.reason_code,new.correlation_id,
    jsonb_build_object('quarantineId',new.quarantine_id,'reasonCode',new.reason_code,
      'legacyState',new.state,'lifecycleState',new.lifecycle_state,'lifecycleVersion',new.lifecycle_version));
  return new;
end $$;

drop trigger if exists quarantine_operational_event on ouf_ingestion.ing_quarantine;
create trigger quarantine_operational_event after insert or update of state,lifecycle_state on ouf_ingestion.ing_quarantine
for each row execute function ouf_ingestion.capture_quarantine_event();
