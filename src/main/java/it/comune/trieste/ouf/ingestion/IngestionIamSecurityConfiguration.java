package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.authorization.PrincipalContext;
import it.comune.trieste.ouf.authorization.TrustedPrincipal;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestFilter;
import org.springframework.web.filter.OncePerRequestFilter;

/** Binds a validated IAM bearer to the SDK's server-only TrustedPrincipal SPI. */
@Configuration(proxyBeanMethods = false)
@org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication(type = org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(name = "ouf.ingestion.iam.enabled", havingValue = "true")
public class IngestionIamSecurityConfiguration {
  private static OAuth2AuthenticationException invalid(String reason) {
    return new OAuth2AuthenticationException(new OAuth2Error("invalid_token", reason, null));
  }

  @Bean JwtDecoder ingestionJwtDecoder(@Value("${ouf.ingestion.iam.issuer}") String issuer,
                                 @Value("${ouf.ingestion.iam.audience}") String audience) {
    var location=java.net.URI.create(issuer);
    if (!"https".equals(location.getScheme()) || location.getHost()==null || location.getUserInfo()!=null
        || location.getQuery()!=null || location.getFragment()!=null || audience.isBlank())
      throw new IllegalStateException("INGESTION_IAM_BINDING_REQUIRED");
    var decoder = NimbusJwtDecoder.withIssuerLocation(issuer).build();
    decoder.setJwtValidator(validators(issuer, audience));
    return decoder;
  }


  static OAuth2TokenValidator<Jwt> validators(String issuer, String audience) {
    OAuth2TokenValidator<Jwt> required = jwt -> {
      boolean present = jwt.getExpiresAt()!=null && jwt.getIssuer()!=null && jwt.getSubject()!=null
          && jwt.getAudience().contains(audience);
      return present ? OAuth2TokenValidatorResult.success()
          : OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token","INGESTION token bindings missing",null));
    };
    return new DelegatingOAuth2TokenValidator<>(JwtValidators.createDefaultWithIssuer(issuer), required);
  }

  @Bean Converter<Jwt, ? extends AbstractAuthenticationToken> ingestionTrustedJwtConverter(
      @Value("${ouf.ingestion.iam.audience}") String audience) {
    return jwt -> {
      try {
      String subject = text(jwt.getSubject());
      String tenant = text(jwt.getClaim("tenant_id"));
      String actor = text(jwt.getClaim("ouf_actor_type"));
      if ("HUMAN_USER".equals(actor)) actor="HUMAN";
      String acr = text(jwt.getClaim("acr"));
      if (subject == null || tenant == null || actor == null || acr == null) throw invalid("INGESTION identity claims missing");
      PrincipalContext.ActorType type;
      try { type = PrincipalContext.ActorType.valueOf(actor); }
      catch (IllegalArgumentException error) { throw invalid("INGESTION actor type invalid"); }
      String alias=text(jwt.getClaim("ouf_subject"));
      if (type==PrincipalContext.ActorType.HUMAN && alias!=null && !alias.equals(subject))
        throw invalid("INGESTION subject claims conflict");
      String service = null;
      if (type == PrincipalContext.ActorType.SERVICE || type == PrincipalContext.ActorType.AI_AGENT) {
        service = text(jwt.getClaim("client_id"));
        if (service == null) service = text(jwt.getClaim("azp"));
        if (service == null) throw invalid("INGESTION service principal missing");
        String client=text(jwt.getClaim("client_id")), azp=text(jwt.getClaim("azp"));
        if (client!=null && azp!=null && !client.equals(azp)) throw invalid("INGESTION workload claims conflict");
      }
      Set<String> scopes = strings(jwt.getClaim("scope"));
      Set<String> roles = roles(jwt);
      var claims = new PrincipalContext.IdentityClaims(roles, acr, strings(jwt.getClaim("amr")),
          jwt.getClaim("auth_time") instanceof Number n ? Instant.ofEpochSecond(n.longValue()) : null);
      var context = new PrincipalContext(subject, tenant, type, service, acr,
          jwt.getIssuer().toString(), audience, scopes, claims);
      Set<GrantedAuthority> authorities = new LinkedHashSet<>();
      for (String scope : scopes) authorities.add(new SimpleGrantedAuthority("SCOPE_" + scope));
      return new TrustedJwtToken(jwt, new TrustedPrincipal(context), authorities);
      } catch (IllegalArgumentException error) { throw invalid("INGESTION identity claims invalid"); }
    };
  }

  @Bean SecurityFilterChain ingestionIamChain(HttpSecurity http,
      Converter<Jwt, ? extends AbstractAuthenticationToken> ingestionTrustedJwtConverter) throws Exception {
    http.csrf(csrf -> csrf.disable());
    http.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
    http.authorizeHttpRequests(auth -> auth
        .requestMatchers("/api/trusted-human/v1/ingestion/**", "/api/ingestion/**", "/api/internal/v1/**").authenticated()
        .anyRequest().permitAll());
    http.oauth2ResourceServer(oauth -> oauth.jwt(jwt -> jwt.jwtAuthenticationConverter(ingestionTrustedJwtConverter)));
    // The servlet API adapter wraps the request and supplies its own principal.
    // Bind the SDK principal on the final request passed to the controller.
    http.addFilterAfter(new TrustedPrincipalBridge(), SecurityContextHolderAwareRequestFilter.class);
    return http.build();
  }

  private static String text(Object value) {
    if (value == null) return null;
    if (!(value instanceof String result)) throw invalid("INGESTION string claim required");
    result=result.trim();
    return result.isEmpty() ? null : result;
  }

  private static Set<String> strings(Object raw) {
    if (raw instanceof String value) {
      if (value.isBlank()) return Set.of();
      return Set.copyOf(java.util.Arrays.asList(value.trim().split("\\s+")));
    }
    if (raw instanceof Collection<?> values) {
      Set<String> result = new LinkedHashSet<>();
      for (Object value : values) if (text(value) != null) result.add(text(value));
      return Set.copyOf(result);
    }
    return Set.of();
  }

  private static Set<String> roles(Jwt jwt) {
    Object canonical = jwt.getClaim("externalRoleRefs"), legacy = jwt.getClaim("external_role_refs");
    Set<String> result = roleArray(canonical == null ? legacy : canonical);
    if (canonical != null && legacy != null && !result.equals(roleArray(legacy))) throw invalid("INGESTION role claims conflict");
    return result;
  }

  private static Set<String> roleArray(Object raw) {
    if (raw == null) return Set.of();
    if (!(raw instanceof Collection<?> values) || values.size() > 32) throw invalid("INGESTION role claims invalid");
    Set<String> result = new LinkedHashSet<>(); int length = 0;
    for (Object value : values) {
      if (!(value instanceof String role) || !role.matches("[A-Za-z0-9_:./-]{1,128}") || !result.add(role))
        throw invalid("INGESTION role claim invalid");
      length += role.length() + 1;
    }
    if (length > 4097) throw invalid("INGESTION role claims too long");
    return Set.copyOf(result);
  }

  static final class TrustedJwtToken extends AbstractAuthenticationToken {
    private final Jwt jwt;
    private final TrustedPrincipal principal;
    TrustedJwtToken(Jwt jwt, TrustedPrincipal principal, Collection<? extends GrantedAuthority> authorities) {
      super(authorities); this.jwt = jwt; this.principal = principal; setAuthenticated(true);
    }
    @Override public Object getCredentials() { return ""; }
    @Override public TrustedPrincipal getPrincipal() { return principal; }
    @Override public String getName() { return principal.getName(); }
  }

  static final class TrustedPrincipalBridge extends OncePerRequestFilter {
    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
        throws ServletException, IOException {
      var authentication = SecurityContextHolder.getContext().getAuthentication();
      if (authentication != null && authentication.isAuthenticated()
          && authentication.getPrincipal() instanceof TrustedPrincipal principal) {
        chain.doFilter(new HttpServletRequestWrapper(request) {
          @Override public Principal getUserPrincipal() { return principal; }
        }, response);
      } else chain.doFilter(request, response);
    }
  }
}

/** Avoid Spring Boot's default generated login when the guarded IAM adapter is disabled. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "ouf.ingestion.iam.enabled", havingValue = "false", matchIfMissing = true)
class IngestionIamDisabledConfiguration {
  @Bean SecurityFilterChain ingestionIamDisabledSentinel(HttpSecurity http) throws Exception {
    http.securityMatcher("/api/trusted-human/v1/ingestion/**", "/api/ingestion/**", "/api/internal/v1/**");
    http.csrf(csrf -> csrf.disable());
    http.authorizeHttpRequests(auth -> auth.anyRequest().denyAll());
    return http.build();
  }
}
