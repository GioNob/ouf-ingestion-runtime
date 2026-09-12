package it.comune.trieste.ouf.ingestion;

import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class HistoricalContractService {
  private final JdbcClient sql;public HistoricalContractService(JdbcClient sql){this.sql=sql;}
  public Map<String,Object> runContracts(UUID run,TrustedAuthorizationContext.Context actor){List<Map<String,Object>> rows=sql.sql("select r.run_id,r.source_id,r.bundle_id,r.bundle_version,r.bundle_checksum,s.snapshot_id,s.snapshot_json#>>'{configuration,sourceSchemaRef}' source_schema_ref,s.snapshot_json#>>'{configuration,sourceSchemaVersion}' source_schema_version,s.snapshot_json#>'{configuration,mappingRefs}' mapping_refs,s.snapshot_json#>>'{configuration,semanticPublicationSetRef}' semantic_publication_set_ref,s.snapshot_json#>>'{configuration,adapterProfileRef}' adapter_profile_ref,s.created_at from ouf_ingestion.ing_run r join ouf_ingestion.runtime_configuration_snapshot s using(run_id) where r.run_id=:r and r.tenant_id=:t").param("r",run).param("t",actor.tenantId()).query().listOfRows();if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"ING_HISTORICAL_CONTRACT_NOT_FOUND");return Collections.unmodifiableMap(new LinkedHashMap<>(rows.getFirst()));}
  public List<Map<String,Object>> lineage(UUID run,TrustedAuthorizationContext.Context actor,int limit){return sql.sql("select l.lineage_id,l.source_object_id,l.bundle_ref,l.adapter_ref,l.evidence_json#>'{contracts}' contract_refs,l.created_at from ouf_ingestion.ing_lineage l join ouf_ingestion.ing_run r using(run_id) where l.run_id=:r and r.tenant_id=:t order by l.created_at,lineage_id limit :n").param("r",run).param("t",actor.tenantId()).param("n",Math.max(1,Math.min(limit,200))).query().listOfRows();}
}
