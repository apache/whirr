package org.apache.whirr.service;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Test;

public class ServiceFactoryTest {
  
  public static class TestService extends Service {

    public TestService(ServiceSpec serviceSpec) {
      super(serviceSpec);
      // TODO Auto-generated constructor stub
    }

    @Override
    public Cluster launchCluster(ClusterSpec clusterSpec) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  @Test
  public void testServiceFactoryIsCreatedFromWhirrProperties() throws IOException {
    ServiceSpec serviceSpec = new ServiceSpec();
    serviceSpec.setName("test-service");
    ServiceFactory factory = new ServiceFactory();
    Service service = factory.create(serviceSpec);
    assertThat(service, instanceOf(TestService.class));
  }
}
