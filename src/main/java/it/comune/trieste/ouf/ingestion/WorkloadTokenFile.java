package it.comune.trieste.ouf.ingestion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Base64;

final class WorkloadTokenFile {
  private WorkloadTokenFile(){}
  static String read(Path token){
    return read(token,Thread::sleep);
  }

  @FunctionalInterface interface Pause {
    void await(long millis) throws InterruptedException;
  }

  static String read(Path token,Pause pause){
    String code="ING_WORKLOAD_TOKEN_INVALID";
    for(int attempt=0;attempt<3;attempt++){
      if(attempt>0)try{pause.await(25L*attempt);}
      catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new RuntimePorts.GatewayFailure("ING_TOKEN_READ_INTERRUPTED",AdapterSpi.ErrorClass.CONFIGURATION);}
      try{
        var before=Files.readAttributes(token,BasicFileAttributes.class);
        byte[] bytes;
        try(var input=Files.newInputStream(token)){bytes=input.readNBytes(16385);}
        var after=Files.readAttributes(token,BasicFileAttributes.class);
        String value=new String(bytes,StandardCharsets.US_ASCII).strip();
        if(bytes.length<=16384 && bytes.length==before.size() && bytes.length==after.size()
            && before.lastModifiedTime().equals(after.lastModifiedTime())
            && java.util.Objects.equals(before.fileKey(),after.fileKey())
            && value.matches("[A-Za-z0-9._~+/-]+=*") && completeToken(value))return value;
        code="ING_WORKLOAD_TOKEN_INVALID";
      }catch(IOException unreadable){code="ING_WORKLOAD_TOKEN_UNREADABLE";}
    }
    throw new RuntimePorts.GatewayFailure(code,AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
  }

  // Recognize a compact JOSE header. Opaque bearer credentials have no length contract.
  private static boolean completeToken(String value){
    if(!value.startsWith("eyJ"))return true;
    String[] parts=value.split("\\.",-1);
    try{
      String header=new String(Base64.getUrlDecoder().decode(parts[0]),StandardCharsets.UTF_8);
      if(!header.contains("\"alg\""))return true;
      boolean encrypted=header.contains("\"enc\"");
      if(parts.length!=(encrypted?5:3))return false;
      for(int i=1;i<parts.length;i++){
        if(encrypted && i==1 && parts[i].isEmpty())continue;
        if(Base64.getUrlDecoder().decode(parts[i]).length==0)return false;
      }
      return true;
    }catch(IllegalArgumentException invalid){return false;}
  }
}
