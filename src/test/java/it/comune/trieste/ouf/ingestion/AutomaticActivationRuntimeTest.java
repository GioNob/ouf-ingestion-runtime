package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.*;

@SpringBootTest @DirtiesContext
class AutomaticActivationRuntimeTest {
 static final Map<String,Object> FILE=PublishedActivationTest.envelope("r2-file",true,1),PULL=PublishedActivationTest.envelope("r2-pull",false,1);
 static final HttpServer SERVER;static final Path TOKEN;
 static {try{TOKEN=Files.createTempFile("r2-workload-",".token");Files.writeString(TOKEN,"ephemeral-workload-test-token");SERVER=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);SERVER.createContext("/",exchange->{try{if(!"Bearer ephemeral-workload-test-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))){exchange.sendResponseHeaders(403,-1);return;}
   String path=exchange.getRequestURI().getPath();Object response;
   if(path.equals("/api/onboarding/v1/runtime/publications"))response=Map.of("items",List.of(FILE,PULL),"nextAfter","");
   else if(path.endsWith("/r2-file/active"))response=FILE;else if(path.endsWith("/r2-pull/active"))response=PULL;
   else if(path.equals("/api/semantic/v1/references:resolve"))response=Map.of("semantic_id","core","semantic_version","1","revision_id","00000000-0000-0000-0000-000000000001","publication_set_id","00000000-0000-0000-0000-000000000002","status","ACTIVE");
   else{exchange.sendResponseHeaders(404,-1);return;}byte[] body=PublishedActivationTest.JSON.writeValueAsBytes(response);exchange.getResponseHeaders().set("Content-Type","application/json");exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);
  }finally{exchange.close();}});SERVER.start();}catch(Exception e){throw new ExceptionInInitializerError(e);}}
 @DynamicPropertySource static void properties(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));r.add("ouf.ingestion.activation.enabled",()->"true");r.add("ouf.ingestion.activation.gateway-url",()->"http://127.0.0.1:"+SERVER.getAddress().getPort());r.add("ouf.ingestion.activation.token-file",()->TOKEN.toString());r.add("ouf.ingestion.activation.tenant-id",()->"tenant-a");r.add("ouf.ingestion.activation.poll-ms",()->"250");}
 @Autowired JdbcClient db;
 @AfterAll static void close()throws Exception{SERVER.stop(0);Files.deleteIfExists(TOKEN);}
 @Test void scheduledBeanStartsFileAndPullWithoutCallingCoordinator()throws Exception{
  long deadline=System.nanoTime()+Duration.ofSeconds(25).toNanos();while(System.nanoTime()<deadline&&count()<2)Thread.sleep(100);assertThat(count()).isEqualTo(2);
  assertThat(db.sql("select phase from ouf_ingestion.ing_run where source_id='r2-file'").query(String.class).single()).isEqualTo("MANAGED_ONCE");
  assertThat(db.sql("select state from ouf_ingestion.ing_schedule where source_id='r2-file'").query(String.class).single()).isEqualTo("DISABLED");
  assertThat(db.sql("select interval_seconds from ouf_ingestion.ing_schedule where source_id='r2-pull'").query(Integer.class).single()).isEqualTo(60);
  Thread.sleep(1000);assertThat(count()).isEqualTo(2);
  assertThat(db.sql("select count(*) from ouf_ingestion.runtime_configuration_snapshot s join ouf_ingestion.ing_run r using(run_id) where r.source_id in ('r2-file','r2-pull') and s.snapshot_json->'configuration'->'publishedBundle'->>'checksum'=r.bundle_checksum").query(Integer.class).single()).isEqualTo(2);
 }
 private int count(){return db.sql("select count(*) from ouf_ingestion.ing_run where source_id in ('r2-file','r2-pull') and state='RUNNING'").query(Integer.class).single();}
 private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
