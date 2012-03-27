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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import static org.jclouds.compute.options.TemplateOptions.Builder.runScript;
import static org.jclouds.scriptbuilder.domain.Statements.appendFile;
import static org.jclouds.scriptbuilder.domain.Statements.createOrOverwriteFile;
import static org.jclouds.scriptbuilder.domain.Statements.interpret;
import static org.jclouds.scriptbuilder.domain.Statements.newStatementList;

import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.aws.ec2.AWSEC2Client;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Joiner;
import static org.jclouds.scriptbuilder.statements.ssh.SshStatements.sshdConfig;

public class BootstrapTemplate {

  private static final Logger LOG =
    LoggerFactory.getLogger(BootstrapTemplate.class);

  public static Template build(
    final ClusterSpec clusterSpec,
    ComputeService computeService,
    StatementBuilder statementBuilder,
    TemplateBuilderStrategy strategy,
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

    TemplateBuilder templateBuilder = computeService.templateBuilder()
      .options(runScript(bootstrap));
    strategy.configureTemplateBuilder(clusterSpec, templateBuilder, instanceTemplate);

    return setSpotInstancePriceIfSpecified(
      computeService.getContext(), clusterSpec, templateBuilder.build(), instanceTemplate
    );
  }

  private static void ensureUserExistsAndAuthorizeSudo(
      StatementBuilder builder, String user, String publicKey, String privateKey
  ) {
    builder.addExport("newUser", user);
    builder.addExport("defaultHome", "/home/users");
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

    if (context != null && context.getProviderSpecificContext().getApi() instanceof AWSEC2Client) {
      float spotPrice = firstPositiveOrDefault(
        0,  /* by default use regular instances */
        instanceTemplate.getAwsEc2SpotPrice(),
        spec.getAwsEc2SpotPrice()
      );
      if (spotPrice > 0) {
        template.getOptions().as(AWSEC2TemplateOptions.class).spotPrice(spotPrice);
      }
    }

    return template;
  }

  private static float firstPositiveOrDefault(float defaultValue, float... listOfValues) {
    for(float value : listOfValues) {
      if (value > 0) return value;
    }
    return defaultValue;
  }

  // must be used inside InitBuilder, as this sets the shell variables used in this statement
  private static Statement ensureUserExistsWithPublicAndPrivateKey(String username,
     String publicKey, String privateKey) {
    // note directory must be created first
    return newStatementList(
      interpret(
        "USER_HOME=$DEFAULT_HOME/$NEW_USER",
        "mkdir -p $USER_HOME/.ssh",
        "useradd --shell /bin/bash -d $USER_HOME $NEW_USER",
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
