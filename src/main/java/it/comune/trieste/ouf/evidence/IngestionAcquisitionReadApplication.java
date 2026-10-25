package it.comune.trieste.ouf.evidence;

import com.zaxxer.hikari.HikariDataSource;
import it.comune.trieste.ouf.ingestion.*;
import java.util.Map;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.MapPropertySource;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/** Separate owner READ entrypoint. Never scans or initializes execution workers. */
@SpringBootConfiguration
@EnableAutoConfiguration
@Import({AcquisitionEvidenceApi.class,AcquisitionEvidenceService.class,TrustedAuthorizationContext.class,OwnerAuthorizationErrors.class,IngestionGatewayReadSecurityConfiguration.class})
public class IngestionAcquisitionReadApplication {
  public static SpringApplication application(){
    var app=new SpringApplication(IngestionAcquisitionReadApplication.class);app.setWebApplicationType(WebApplicationType.SERVLET);
    app.addInitializers(context->context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("enforced-owner-read-only",Map.of(
      "spring.flyway.enabled","false","spring.sql.init.mode","never",
      "ouf.ingestion.execution.enabled","false","management.endpoints.web.exposure.include","health","management.endpoint.health.show-details","never",
      "management.endpoint.health.probes.enabled","true",
      "management.endpoint.health.group.readiness.include","readinessState,db,acquisitionAuthorization"))));
    return app;
  }
  public static void main(String[] args){application().run(args);}
  @Bean static BeanPostProcessor readOnlyPool(){return new BeanPostProcessor(){
    @Override public Object postProcessBeforeInitialization(Object bean,String name){
      if(bean instanceof HikariDataSource ds){
        if(ds.isRunning())throw new IllegalStateException("EVIDENCE_POOL_ALREADY_STARTED");
        ds.setReadOnly(true);ds.setAutoCommit(false);ds.setMaximumPoolSize(2);ds.setMinimumIdle(0);
        ds.setConnectionTimeout(5000);ds.setValidationTimeout(2000);
      }
      return bean;
    }
  };}
  @Bean org.springframework.boot.health.contributor.HealthIndicator acquisitionAuthorization(it.comune.trieste.ouf.authorization.LocalAuthorization runtime){return ()->runtime.health().ready()?org.springframework.boot.health.contributor.Health.up().build():org.springframework.boot.health.contributor.Health.down().build();}
  @Bean org.springframework.security.authentication.AuthenticationManager receiptOnlyAuthentication(){return authentication->{throw new org.springframework.security.authentication.BadCredentialsException("EVIDENCE_RECEIPT_REQUIRED");};}
  @Bean @Order(2) SecurityFilterChain evidenceOnlyChain(HttpSecurity http)throws Exception{
    http.csrf(c->c.disable());http.sessionManagement(s->s.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
    http.authorizeHttpRequests(a->a.requestMatchers(HttpMethod.POST,"/api/internal/v1/ingestion/operations/acquisition").permitAll()
      .requestMatchers(HttpMethod.GET,"/actuator/health","/actuator/health/**").permitAll().anyRequest().denyAll());
    return http.build();
  }
}
