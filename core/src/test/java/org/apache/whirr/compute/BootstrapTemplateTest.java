/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.compute;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.MalformedURLException;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.aws.ec2.compute.AWSEC2ComputeService;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.compute.options.TemplateOptions;
import org.junit.Before;
import org.junit.Test;

public class BootstrapTemplateTest {

  private StatementBuilder statementBuilder;

  @Before
  public void setUp() throws Exception {
    statementBuilder = new StatementBuilder();
  }

  @Test
  public void testSetGlobalSpotPrice() throws Exception {
    ClusterSpec clusterSpec = buildClusterSpecWith(ImmutableMap.of(
      "whirr.instance-templates", "1 role1",
      "whirr.aws-ec2-spot-price", "0.3"
    ));
    assertSpotPriceIs(clusterSpec, "role1", 0.3f);
  }

  @Test
  public void testOverrideSpotPrice() throws Exception {
    ClusterSpec clusterSpec = buildClusterSpecWith(ImmutableMap.of(
      "whirr.instance-templates", "1 role1",
      "whirr.aws-ec2-spot-price", "0.1",
      "whirr.templates.role1.aws-ec2-spot-price", "0.3"
    ));
    assertSpotPriceIs(clusterSpec, "role1", 0.3f);
  }

  @Test
  public void testDifferentPrices() throws Exception {
    ClusterSpec clusterSpec = buildClusterSpecWith(ImmutableMap.of(
      "whirr.instance-templates", "1 role1, 1 role2",
      "whirr.aws-ec2-spot-price", "0.1",
      "whirr.templates.role2.aws-ec2-spot-price", "0.3"
    ));
    assertSpotPriceIs(clusterSpec, "role1", 0.1f);
    assertSpotPriceIs(clusterSpec, "role2", 0.3f);
  }

  private ClusterSpec buildClusterSpecWith(Map<String, String> overrides) throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("whirr.image-id", "us-east-1/ami-123");
    for (String key : overrides.keySet()) {
      config.setProperty(key, overrides.get(key));
    }
    return ClusterSpec.withTemporaryKeys(config);
  }

  @SuppressWarnings("unchecked")
private void assertSpotPriceIs(
    ClusterSpec clusterSpec, final String templateGroup, float spotPrice
  ) throws MalformedURLException {

    InstanceTemplate instanceTemplate = getOnlyElement(filter(
      clusterSpec.getInstanceTemplates(),
      new Predicate<InstanceTemplate>() {
        private Joiner plusJoiner = Joiner.on("+");

        @Override
        public boolean apply(InstanceTemplate group) {
          return plusJoiner.join(group.getRoles()).equals(templateGroup);
        }
      }));

    ComputeService computeService = mock(AWSEC2ComputeService.class);
    ComputeServiceContext context = mock(ComputeServiceContext.class);
    when(computeService.getContext()).thenReturn(context);
    when(context.getComputeService()).thenReturn(computeService);

    TemplateBuilder templateBuilder = mock(TemplateBuilder.class);
    when(computeService.templateBuilder()).thenReturn(templateBuilder);
    when(templateBuilder.from((TemplateBuilderSpec)any())).thenReturn(templateBuilder);
    when(templateBuilder.options((TemplateOptions)any())).thenReturn(templateBuilder);

    Template template = mock(Template.class);
    TemplateOptions options = mock(TemplateOptions.class);
    Image image = mock(Image.class);
    when(templateBuilder.build()).thenReturn(template);
    when(template.getOptions()).thenReturn(options);
    when(template.getImage()).thenReturn(image);

    AWSEC2TemplateOptions awsEec2TemplateOptions = mock(AWSEC2TemplateOptions.class);
    when(options.as((Class<TemplateOptions>) any())).thenReturn(awsEec2TemplateOptions);

    BootstrapTemplate.build(clusterSpec, computeService,
      statementBuilder, instanceTemplate);

    verify(awsEec2TemplateOptions).spotPrice(spotPrice);
  }
}
