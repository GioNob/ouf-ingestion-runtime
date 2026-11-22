package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/** Separate JVM entry point. Does not start Spring, Flyway, a scheduler or a data-plane run. */
public final class FrozenConfigurationProbeMain {
  public static void main(String[] args){
    try{
      if(args.length!=2||!"--properties".equals(args[0]))throw new IllegalArgumentException("ING_COMPAT_PROPERTIES_REQUIRED");
      Properties properties=new Properties();
      try(Reader in=Files.newBufferedReader(Path.of(args[1]))){properties.load(in);}
      String url=required(properties,"ouf.ingestion.activation.gateway-url"),token=required(properties,"ouf.ingestion.activation.token-file"),tenant=required(properties,"ouf.ingestion.activation.tenant-id");
      byte[] input=System.in.readNBytes(3_145_729);
      if(input.length==0||input.length>3_145_728)throw new IllegalArgumentException("ING_COMPAT_INPUT_SIZE_INVALID");
      var json=new ObjectMapper();
      @SuppressWarnings("unchecked") Map<String,Object> candidate=json.readValue(input,Map.class);
      var pacing=new GatewayPacing(properties::getProperty);
      var objects=new ExecutionGatewayClient(json,null,url,token,pacing);
      var semantics=new PublicationGatewayClient(json,url,token,tenant,pacing);
      System.out.println(json.writeValueAsString(new FrozenConfigurationProbe(json,objects,semantics).probe(candidate)));
    }catch(Exception error){
      String code=error.getMessage();
      if(code==null||!code.matches("ING_[A-Z0-9_]{1,80}"))code="ING_COMPAT_PROBE_FAILED";
      System.out.println("{\"schema\":\"ouf.ingestion.compatibility-probe.v1\",\"status\":\"BLOCKED\",\"code\":\""+code+"\",\"attestationSubmitted\":false}");
      System.exit(1);
    }
  }
  static String required(Properties properties,String key){
    String value=properties.getProperty(key);
    if(value==null||value.isBlank()||value.contains("${"))throw new IllegalArgumentException("ING_COMPAT_TRANSPORT_CONFIG_REQUIRED");
    return value.strip();
  }
}
