package it.comune.trieste.ouf.ingestion;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
@SpringBootApplication @EnableScheduling
public class IngestionRuntimeApplication {public static void main(String[] args){SpringApplication.run(IngestionRuntimeApplication.class,args);}}
