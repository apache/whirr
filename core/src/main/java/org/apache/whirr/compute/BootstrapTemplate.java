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

import static org.jclouds.ec2.domain.RootDeviceType.EBS;
import static org.jclouds.scriptbuilder.domain.Statements.appendFile;
import static org.jclouds.scriptbuilder.domain.Statements.createOrOverwriteFile;
import static org.jclouds.scriptbuilder.domain.Statements.interpret;
import static org.jclouds.scriptbuilder.domain.Statements.newStatementList;
import static org.jclouds.scriptbuilder.statements.ssh.SshStatements.sshdConfig;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.aws.ec2.compute.AWSEC2ComputeService;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.ec2.compute.EC2ComputeService;
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.ec2.compute.predicates.EC2ImagePredicates;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapTemplate {

  private static final Logger LOG =
    LoggerFactory.getLogger(BootstrapTemplate.class);

  public static Template build(
    final ClusterSpec clusterSpec,
    ComputeService computeService,
    StatementBuilder statementBuilder,
    InstanceTemplate instanceTemplate
  ) {
    String name = "bootstrap-" + Joiner.on('_').join(instanceTemplate.getRoles());

    LOG.info("Configuring template for {}", name);

    statementBuilder.name(name);
    ensureUserExistsAndAuthorizeSudo(statementBuilder, clusterSpec.getClusterUser(),
        clusterSpec.getPublicKey(), clusterSpec.getPrivateKey());
    Statement bootstrap = statementBuilder.build(clusterSpec);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Running script {}:\n{}", name, bootstrap.render(OsFamily.UNIX));
    }

    TemplateBuilder templateBuilder = computeService.templateBuilder().from(
        instanceTemplate.getTemplate() != null ? instanceTemplate.getTemplate() :
        clusterSpec.getTemplate());
    Template template = templateBuilder.build();
    template.getOptions().runScript(bootstrap);
    return setSpotInstancePriceIfSpecified(
      computeService.getContext(), clusterSpec, template, instanceTemplate
    );
  }

  private static void ensureUserExistsAndAuthorizeSudo(
      StatementBuilder builder, String user, String publicKey, String privateKey
  ) {
    builder.addExport("NEW_USER", user);
    builder.addExport("DEFAULT_HOME", "/home/users");
    builder.addStatement(0, newStatementList(
        ensureUserExistsWithPublicAndPrivateKey(user, publicKey, privateKey),
        makeSudoersOnlyPermitting(user),
        disablePasswordBasedAuth())
    );
  }

  /**
   * Set maximum spot instance price based on the configuration
   */
  private static Template setSpotInstancePriceIfSpecified(
      ComputeServiceContext context, ClusterSpec spec, Template template, InstanceTemplate instanceTemplate
  ) {

    if (AWSEC2ComputeService.class.isInstance(context.getComputeService())) {
      template.getOptions().as(AWSEC2TemplateOptions.class)
            .spotPrice(instanceTemplate.getAwsEc2SpotPrice() != null ? instanceTemplate.getAwsEc2SpotPrice() :
                                                                       spec.getAwsEc2SpotPrice());
    }

    return mapEphemeralIfImageIsEBSBacked(context, spec, template, instanceTemplate);
  }

    /**
     * If this is an EBS-backed volume, map the ephemeral device.
     */
    private static Template mapEphemeralIfImageIsEBSBacked(ComputeServiceContext context,
                                                           ClusterSpec spec,
                                                           Template template,
                                                           InstanceTemplate instanceTemplate) {
      if (EC2ComputeService.class.isInstance(context.getComputeService())) {
        if (EC2ImagePredicates.rootDeviceType(EBS).apply(template.getImage())) {
          template.getOptions().as(EC2TemplateOptions.class).mapEphemeralDeviceToDeviceName("/dev/sdc", "ephemeral1");
        }
      }
      return setPlacementGroup(context, spec, template, instanceTemplate);
    }
    
    /**
     * Set the placement group, if desired - if it doesn't already exist, create it.
     */
    private static Template setPlacementGroup(ComputeServiceContext context, ClusterSpec spec,
                                              Template template, InstanceTemplate instanceTemplate) {
      if (AWSEC2ComputeService.class.isInstance(context.getComputeService())) {
        if (spec.getAwsEc2PlacementGroup() != null) {
          template.getOptions().as(AWSEC2TemplateOptions.class).placementGroup(spec.getAwsEc2PlacementGroup());
        }
      }

      return template;
    }

  // must be used inside InitBuilder, as this sets the shell variables used in this statement
  private static Statement ensureUserExistsWithPublicAndPrivateKey(String username,
     String publicKey, String privateKey) {
    // note directory must be created first
    return newStatementList(
      interpret(
        "USER_HOME=$DEFAULT_HOME/$NEW_USER",
        "mkdir -p $USER_HOME/.ssh",
        "useradd -u 2000 --shell /bin/bash -d $USER_HOME $NEW_USER",
        "[ $? -ne 0 ] && USER_HOME=$(grep $NEW_USER /etc/passwd | cut -d \":\" -f6)\n"),
      appendFile(
        "$USER_HOME/.ssh/authorized_keys",
        Splitter.on('\n').split(publicKey)),
      createOrOverwriteFile(
        "$USER_HOME/.ssh/id_rsa.pub",
        Splitter.on('\n').split(publicKey)),
      createOrOverwriteFile(
        "$USER_HOME/.ssh/id_rsa",
        Splitter.on('\n').split(privateKey)),
      interpret(
        "chmod 400 $USER_HOME/.ssh/*",
        "chown -R $NEW_USER $USER_HOME\n"));
  }

  // must be used inside InitBuilder, as this sets the shell variables used in this statement
  private static Statement makeSudoersOnlyPermitting(String username) {
    return newStatementList(
      interpret(
        "rm /etc/sudoers",
        "touch /etc/sudoers",
        "chmod 0440 /etc/sudoers",
        "chown root /etc/sudoers\n"),
      appendFile(
        "/etc/sudoers",
        ImmutableSet.of(
          "root ALL = (ALL) ALL",
          "%adm ALL = (ALL) ALL",
          username + " ALL = (ALL) NOPASSWD: ALL")
        )
    );
  }

  private static Statement disablePasswordBasedAuth() {
    return sshdConfig(ImmutableMap.of("PasswordAuthentication","no"));
  }
}
