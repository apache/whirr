package org.apache.whirr.service;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.internal.Maps;

/**
 * This class is used to create {@link Service} instances.
 * <p>
 * <i>Implementation note.</i> {@link Service} implementations are discovered
 * using a Service Provider
 * Interface (SPI), where each provider JAR on the classpath contains a
 * <i>whirr.properties</i>
 * file containing the following two properties:
 * <ul>
 * <li><code>whirr.service.name</code> - the name of the service, for example,
 * <code>hadoop</code></li>
 * <li><code>whirr.service.class</code> - the fully-qualified classname of the
 * implementation of {@link Service}, for example,
 * <code>org.apache.whirr.service.hadoop.HadoopService</code></li>
 * </ul>  
 */
public class ServiceFactory {

  private static final Logger LOG =
    LoggerFactory.getLogger(ServiceFactory.class);
  private Map<String, Class<Service>> serviceMap = Maps.newHashMap();

  @SuppressWarnings("unchecked")
  public ServiceFactory() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    Enumeration<URL> resources = classLoader.getResources("whirr.properties");

    for (URL url : Collections.list(resources)) {
      InputStream in = null;
      try {
        in = url.openStream();
        Properties properties = new Properties();
        properties.load(in);
        String serviceName = properties.getProperty("whirr.service.name");
        String serviceClassName = properties.getProperty("whirr.service.class");
        if (serviceName == null) {
          LOG.warn("Property whirr.service.name not set.");
          continue;
        }
        if (serviceClassName == null) {
          LOG.warn("Property whirr.service.class not set.");
          continue;
        }
        Class<Service> serviceClass = (Class<Service>) Class
            .forName(serviceClassName);
        serviceMap.put(serviceName, serviceClass);
      } catch (IOException e) {
        LOG.warn("Problem reading whirr.properties.", e);
        continue;
      } catch (ClassNotFoundException e) {
        LOG.warn("ServiceFactory class not found.", e);
        continue;
      } finally {
        if (in != null) {
          try {
            in.close();
          } catch (IOException e) {
            LOG.warn("Problem closing stream.", e);
            continue;
          }
        }
      }
    }
  }

  /**
   * Create an instance of a {@link Service} according to the given
   * {@link ServiceSpec}.
   */
  public Service create(ServiceSpec serviceSpec) {
    Class<Service> serviceClass = serviceMap.get(serviceSpec.getName());
    if (serviceClass == null) {
      return null;
    }
    try {
      Constructor<Service> constructor = serviceClass
          .getConstructor(ServiceSpec.class);
      return constructor.newInstance(serviceSpec);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
