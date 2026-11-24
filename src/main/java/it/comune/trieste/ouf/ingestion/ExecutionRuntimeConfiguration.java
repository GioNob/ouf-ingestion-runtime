package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;

@Configuration(proxyBeanMethods=false)
@ConditionalOnProperty(name="ouf.ingestion.execution.enabled",havingValue="true")
public class ExecutionRuntimeConfiguration {
  @Bean RuntimePorts.AdapterResolver executionAdapters(ExecutionGatewayClient gateway,ObjectMapper json){
    return bundle->{
      AdapterSpi adapter=switch(bundle.acquisitionMode()){
        case "REST_JSON" -> new GatewayJsonAdapter(gateway,json);
        case "WFS" -> new GatewayWfsAdapter(gateway);
        case "INTERNAL_MANAGED_CSV","INTERNAL_MANAGED_XLSX","INTERNAL_MANAGED_GEOPACKAGE" ->
            managedAdapter(bundle,gateway,"ING_ADAPTER_ID_MISMATCH");
        default -> throw new IllegalArgumentException("ING_ADAPTER_MODE_UNSUPPORTED");
      };
      if(!adapter.adapterId().equals(bundle.configuration().get("adapterId")))throw new IllegalStateException("ING_ADAPTER_ID_MISMATCH");
      return adapter;
    };
  }
  static AdapterSpi managedAdapter(ExecutionBundle bundle,RuntimePorts.ManagedObjectPort objects,String unsupported){
    Object id=bundle.configuration().get("adapterId");
    switch(bundle.acquisitionMode()){
      case "INTERNAL_MANAGED_XLSX":
        if(ManagedXlsxAdapter.ID.equals(id))return new ManagedXlsxAdapter(objects);
        if("managed-tabular-v1".equals(id))return new ManagedTabularAdapter(objects);
        break;
      case "INTERNAL_MANAGED_CSV":
        if("managed-tabular-v1".equals(id))return new ManagedTabularAdapter(objects);
        break;
      case "INTERNAL_MANAGED_GEOPACKAGE":
        if("managed-geopackage-v1".equals(id))return new ManagedGeoPackageAdapter(objects);
        break;
    }
    throw new IllegalArgumentException(unsupported);
  }
  @Bean CanonicalRecordPipeline executionPipeline(ObjectMapper json,FrozenContractValidator contracts,DurablePipelineRepository durable,QuarantineService quarantine,ExecutionGatewayClient gateway){return new CanonicalRecordPipeline(json,contracts,durable,quarantine,gateway);}
  @Bean RunExecutionWorker executionWorker(RunExecutionRepository runs,RuntimePorts.AdapterResolver adapters,CanonicalRecordPipeline pipeline,SchemaSurveillanceService schemas,RunStateRepository health,
      org.springframework.beans.factory.ObjectProvider<PublicationGatewayClient> publications){
    return new RunExecutionWorker(runs,adapters,pipeline,schemas,health,snapshot->{
      var gateway=publications.getIfAvailable();
      if(gateway==null)throw new IllegalStateException("ING_PUBLICATION_VERIFIER_REQUIRED");
      gateway.verifyForRetry(snapshot);
    });
  }
  @Bean OutboxDispatcher executionDispatcher(DurablePipelineRepository repository,ExecutionGatewayClient gateway,IngestionMetrics metrics){return new OutboxDispatcher(repository,gateway,metrics);}
}
