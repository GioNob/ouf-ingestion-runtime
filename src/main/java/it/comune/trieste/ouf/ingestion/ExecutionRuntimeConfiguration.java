package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;

@Configuration(proxyBeanMethods=false)
@ConditionalOnProperty(name="ouf.ingestion.execution.enabled",havingValue="true")
public class ExecutionRuntimeConfiguration {
  @Bean RuntimePorts.AdapterResolver executionAdapters(ExecutionGatewayClient gateway,ObjectMapper json){
    return bundle->switch(bundle.acquisitionMode()){
      case "INTERNAL_MANAGED_SHAPEFILE" -> new ManagedShapefileAdapter(gateway);
      case "INTERNAL_MANAGED_ACCESS" -> new ManagedAccessAdapter(gateway);
      case "INTERNAL_MANAGED_GEOPACKAGE" -> new ManagedGeoPackageAdapter(gateway);
      case "INTERNAL_MANAGED_CSV","INTERNAL_MANAGED_XLSX" -> new ManagedTabularAdapter(gateway);
      case "REST_JSON" -> new GatewayJsonAdapter(gateway,json);
      case "WFS" -> new GatewayWfsAdapter(gateway);
      default -> throw new IllegalArgumentException("ING_ADAPTER_MODE_UNSUPPORTED");
    };
  }
  @Bean CanonicalRecordPipeline executionPipeline(ObjectMapper json,FrozenContractValidator contracts,DurablePipelineRepository durable,QuarantineService quarantine,ExecutionGatewayClient gateway){return new CanonicalRecordPipeline(json,contracts,durable,quarantine,gateway);}
  @Bean RunExecutionWorker executionWorker(RunExecutionRepository runs,RuntimePorts.AdapterResolver adapters,CanonicalRecordPipeline pipeline,SchemaSurveillanceService schemas,RunStateRepository health){return new RunExecutionWorker(runs,adapters,pipeline,schemas,health);}
  @Bean OutboxDispatcher executionDispatcher(DurablePipelineRepository repository,ExecutionGatewayClient gateway,IngestionMetrics metrics){return new OutboxDispatcher(repository,gateway,metrics);}
}
