package it.comune.trieste.ouf.pairwise;

import it.comune.trieste.ouf.ingestion.IngestionRuntimeApplication;
import it.comune.trieste.ouf.authorization.*;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.*;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/** Identity-only disposable launcher. Production activation, readiness and execution are unchanged. */
public class ChatbotIngestionFixture {
  public static void main(String[] args){SpringApplication.run(new Class<?>[]{IngestionRuntimeApplication.class,Fixture.class},args);}
  @Configuration(proxyBeanMethods=false) public static class Fixture {
    // Test identity transport only: the production disabled-IAM sentinel remains
    // unchanged. This exact route still requires the ephemeral HUMAN token below
    // and its real owner capability/resource authorization inside the controller.
    @Bean @Order(-1) SecurityFilterChain verticalIdentityTransport(HttpSecurity http)throws Exception{
      http.securityMatcher("/api/trusted-human/v1/ingestion/onboarding-readiness");
      http.csrf(csrf->csrf.disable());
      http.authorizeHttpRequests(auth->auth.anyRequest().permitAll());
      return http.build();
    }
    @Bean FilterRegistrationBean<Filter> verticalHuman(){
      String token=System.getenv("OUF_PAIRWISE_HUMAN_TOKEN");if(token==null||!token.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Ephemeral HUMAN fixture token required");
      var caps=Set.of("ouf.onboarding.configuration.write");var authorization=TestAuthorization.runtime("fixture-human","HUMAN",caps);
      Filter filter=(input,output,chain)->{
        var request=(HttpServletRequest)input;String bearer=request.getHeader("Authorization");
        if(bearer==null||!MessageDigest.isEqual(bearer.getBytes(StandardCharsets.UTF_8),("Bearer "+token).getBytes(StandardCharsets.UTF_8))){((HttpServletResponse)output).sendError(401);return;}
        request.getServletContext().setAttribute(ServletAuthorization.RUNTIME,authorization);
        chain.doFilter(new HttpServletRequestWrapper(request){@Override public Principal getUserPrincipal(){return TestAuthorization.principal("fixture-human","HUMAN",caps);}},output);
      };
      var bean=new FilterRegistrationBean<>(filter);bean.addUrlPatterns("/api/trusted-human/v1/ingestion/onboarding-readiness");bean.setOrder(0);return bean;
    }
  }
}
