package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.receipt.SummaryReceiptFilter;
import java.time.Clock;
import java.util.Set;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestFilter;

/** Opt-in APISIX producer boundary, ahead of the IAM catch-all chain. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(name = "ouf.summary.enabled", havingValue = "true")
public class SummaryIdentityConfiguration {
  @Bean Clock operationalReceiptClock() { return Clock.systemUTC(); }

  @Bean @Order(-1)
  SecurityFilterChain operationalSummaryIdentity(HttpSecurity http, Environment env,
      @Qualifier("operationalReceiptClock") Clock clock) throws Exception {
    var paths = Set.of(SummaryReceiptFilter.SUMMARY_PATH, SummaryReceiptFilter.INCIDENTS_PATH);
    // Exact URI matching also guards unsupported methods; no JWT fallback on these paths.
    http.securityMatcher(request -> paths.contains(request.getRequestURI()));
    http.csrf(csrf -> csrf.disable());
    http.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
    http.authorizeHttpRequests(auth -> auth.anyRequest().authenticated());
    // Construct inside the chain: never register this filter globally with the servlet container.
    http.addFilterAfter(new SummaryReceiptFilter(
        env.getProperty("ouf.summary.tenant-id", ""), env.getProperty("ouf.summary.issuer", ""),
        env.getProperty("ouf.summary.audience", ""), env.getProperty("ouf.summary.workload", ""),
        env.getProperty("ouf.summary.receipt-key-file"), clock),
        SecurityContextHolderAwareRequestFilter.class);
    return http.build();
  }
}
