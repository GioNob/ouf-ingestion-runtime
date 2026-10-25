package it.comune.trieste.ouf.ingestion;

import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** CI-only lifecycle for a packaged, isolated READ image against synthetic inputs. */
final class PackagedReadProcess implements AutoCloseable {
  final String name="ouf-read-ci-"+UUID.randomUUID();
  final int port;
  final Process process;
  final Path log;
  PackagedReadProcess(Path directory,List<String> arguments,String dbPrefix)throws Exception{
    try(var socket=new java.net.ServerSocket(0)){port=socket.getLocalPort();}
    log=directory.resolve("container.log");
    java.nio.file.Files.setPosixFilePermissions(directory,java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));
    try(var files=Files.list(directory)){for(Path file:files.toList())Files.setPosixFilePermissions(file,java.nio.file.attribute.PosixFilePermissions.fromString("r--r--r--"));}
    var cmd=new ArrayList<>(List.of("docker","run","--name",name,"--network","host","--read-only","--cap-drop","ALL","--security-opt","no-new-privileges:true","--pids-limit","128","--memory","512m","--tmpfs","/tmp:rw,nosuid,noexec,size=64m","--mount","type=bind,src="+directory+",dst="+directory+",readonly"));
    for(String suffix:List.of("URL","USER","PASSWORD")){
      String env=dbPrefix+suffix;cmd.add("--env");cmd.add(env+"="+Objects.requireNonNull(System.getenv(env),env));
    }
    cmd.add(Objects.requireNonNull(System.getenv("EVIDENCE_READ_IMAGE")));cmd.add("--server.port="+port);cmd.addAll(arguments);
    process=new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
  }
  HttpClient client(){return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();}
  void ready()throws Exception{
    long end=System.nanoTime()+Duration.ofSeconds(60).toNanos();
    var client=client();
    while(System.nanoTime()<end && process.isAlive()){
      try{var response=client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/actuator/health/readiness")).timeout(Duration.ofSeconds(2)).GET().build(),HttpResponse.BodyHandlers.ofString());if(response.statusCode()==200)return;}
      catch(java.io.IOException ignored){}Thread.sleep(200);
    }
    throw new AssertionError("it.comune.trieste.ouf.ingestionD_READINESS_REQUIRED: "+Files.readString(log));
  }
  @Override public void close()throws Exception{
    var cleanup=new ProcessBuilder("docker","rm","-f",name).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
    if(!cleanup.waitFor(15,TimeUnit.SECONDS))cleanup.destroyForcibly();
    if(!process.waitFor(10,TimeUnit.SECONDS))process.destroyForcibly();
  }
}
