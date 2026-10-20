package it.comune.trieste.ouf.ingestion;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Installation bootstrap mode: reject migration auto-configuration before singleton creation. */
@Component
@ConditionalOnProperty(name="ouf.ingestion.startup.schema-mode",havingValue="VALIDATE_ONLY")
public final class IngestionValidateOnlyStartup implements BeanFactoryPostProcessor,EnvironmentAware {
  private Environment environment;
  @Override public void setEnvironment(Environment environment){this.environment=environment;}
  @Override public void postProcessBeanFactory(ConfigurableListableBeanFactory beans)throws BeansException{
    if(!Boolean.FALSE.equals(environment.getProperty("spring.flyway.enabled",Boolean.class)))
      throw new BeanInitializationException("ING_STARTUP_MIGRATE_FORBIDDEN");
  }
}
