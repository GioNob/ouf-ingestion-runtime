package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.authorization.TrustedPrincipal;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.*;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestFilter;
import org.springframework.web.filter.OncePerRequestFilter;

/** A distinct authenticated owner boundary; IAM/HUMAN and mutation paths remain unchanged. */
@Configuration(proxyBeanMethods=false)
@ConditionalOnWebApplication(type=ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(name="ouf.ingestion.gateway-read.enabled",havingValue="true")
public class IngestionGatewayReadSecurityConfiguration {
  @Bean GatewayReadReceiptVerifier gatewayReadReceiptVerifier(
      @Value("${ouf.ingestion.gateway-read.key-file}") String file,
      @Value("${ouf.ingestion.gateway-read.issuer}") String issuer,
      @Value("${ouf.ingestion.gateway-read.audience}") String audience,
      @Value("${ouf.ingestion.gateway-read.workload}") String workload) {
    return new GatewayReadReceiptVerifier(file,issuer,audience,workload);
  }
  @Bean @Order(0) SecurityFilterChain ingestionGatewayReadChain(HttpSecurity http,GatewayReadReceiptVerifier verifier)throws Exception {
    http.securityMatcher(GatewayReadReceiptVerifier.PREFIX+"status",GatewayReadReceiptVerifier.PREFIX+"history");
    http.csrf(c->c.disable());http.sessionManagement(s->s.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
    http.exceptionHandling(e->e.authenticationEntryPoint((request,response,error)->response.sendError(401)));
    http.authorizeHttpRequests(a->a.anyRequest().authenticated());
    // Run after the servlet security adapter so the final controller request sees
    // the server-created TrustedPrincipal, never a client-supplied header.
    http.addFilterAfter(new ReceiptFilter(verifier),SecurityContextHolderAwareRequestFilter.class);
    return http.build();
  }
  static final class ReceiptFilter extends OncePerRequestFilter {
    private final GatewayReadReceiptVerifier verifier;
    ReceiptFilter(GatewayReadReceiptVerifier verifier){this.verifier=verifier;}
    static String header(HttpServletRequest request,String name) {
      var values=request.getHeaders(name);if(values==null||!values.hasMoreElements())throw GatewayReadReceiptVerifier.denied();
      String value=values.nextElement();if(values.hasMoreElements())throw GatewayReadReceiptVerifier.denied();return value;
    }
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)throws ServletException,IOException {
      if(!"POST".equals(request.getMethod())){response.sendError(405);return;}
      if(request.getQueryString()!=null){response.sendError(400);return;}
      String type=request.getContentType();if(type==null||!type.split(";",2)[0].trim().equalsIgnoreCase("application/json")){response.sendError(415);return;}
      byte[] body=request.getInputStream().readNBytes(65537);if(body.length>65536){response.sendError(413);return;}
      final TrustedPrincipal principal;
      try {
        if(request.getHeader("Authorization")!=null)throw GatewayReadReceiptVerifier.denied();
        var context=verifier.verify(header(request,GatewayReadReceiptVerifier.HEADER),request.getMethod(),request.getRequestURI(),body,
            header(request,"X-Correlation-ID"),header(request,"X-Tool-Attempt-ID"),header(request,"Idempotency-Key"));
        principal=new TrustedPrincipal(context);
      }catch(SecurityException invalid){response.sendError(401,"INGESTION_GATEWAY_READ_RECEIPT_INVALID");return;}
      var old=SecurityContextHolder.getContext();var security=SecurityContextHolder.createEmptyContext();
      security.setAuthentication(new UsernamePasswordAuthenticationToken(principal,"",List.of()));SecurityContextHolder.setContext(security);
      try {chain.doFilter(new BodyRequest(request,body,principal),response);}
      finally {SecurityContextHolder.setContext(old);}
    }
  }
  static final class BodyRequest extends HttpServletRequestWrapper {
    private final byte[] body;private final Principal principal;
    BodyRequest(HttpServletRequest request,byte[] body,Principal principal){super(request);this.body=body;this.principal=principal;}
    @Override public Principal getUserPrincipal(){return principal;}
    @Override public String getRemoteUser(){return principal.getName();}
    @Override public int getContentLength(){return body.length;}
    @Override public long getContentLengthLong(){return body.length;}
    @Override public BufferedReader getReader(){return new BufferedReader(new InputStreamReader(getInputStream(),StandardCharsets.UTF_8));}
    @Override public ServletInputStream getInputStream(){
      var input=new ByteArrayInputStream(body);
      return new ServletInputStream(){
        @Override public int read(){return input.read();}
        @Override public int read(byte[] target,int offset,int length){return input.read(target,offset,length);}
        @Override public boolean isFinished(){return input.available()==0;}
        @Override public boolean isReady(){return true;}
        @Override public void setReadListener(ReadListener listener){throw new IllegalStateException("SYNCHRONOUS_INGESTION_READ_REQUIRED");}
      };
    }
  }
}
