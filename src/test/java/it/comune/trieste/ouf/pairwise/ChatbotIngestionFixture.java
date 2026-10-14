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

/** Identity-only disposable launcher. Production activation, readiness and execution are unchanged. */
public class ChatbotIngestionFixture {
  public static void main(String[] args){SpringApplication.run(new Class<?>[]{IngestionRuntimeApplication.class,Fixture.class},args);}
  @Configuration(proxyBeanMethods=false) public static class Fixture {
    @Bean FilterRegistrationBean<Filter> verticalHuman(){
      String token=System.getenv("OUF_PAIRWISE_HUMAN_TOKEN");if(token==null||!token.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Ephemeral HUMAN fixture token required");
      var caps=Set.of("ouf.onboarding.configuration.write");var authorization=TestAuthorization.runtime("fixture-human","HUMAN",caps);
      Filter filter=(input,output,chain)->{
        var request=(HttpServletRequest)input;String bearer=request.getHeader("Authorization");
        if(bearer==null||!MessageDigest.isEqual(bearer.getBytes(StandardCharsets.UTF_8),("Bearer "+token).getBytes(StandardCharsets.UTF_8))){((HttpServletResponse)output).sendError(401);return;}
        request.getServletContext().setAttribute(ServletAuthorization.RUNTIME,authorization);
        chain.doFilter(new HttpServletRequestWrapper(request){@Override public Principal getUserPrincipal(){return TestAuthorization.principal("fixture-human","HUMAN",caps);}},output);
      };
      var bean=new FilterRegistrationBean<>(filter);bean.addUrlPatterns("/api/trusted-human/v1/ingestion/*");bean.setOrder(-100);return bean;
    }
  }
}
