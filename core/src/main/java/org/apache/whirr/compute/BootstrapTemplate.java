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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.aws.ec2.AWSEC2Client;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.scriptbuilder.InitBuilder;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;

import static org.jclouds.compute.options.TemplateOptions.Builder.runScript;
import static org.jclouds.scriptbuilder.domain.Statements.appendFile;
import static org.jclouds.scriptbuilder.domain.Statements.interpret;
import static org.jclouds.scriptbuilder.domain.Statements.newStatementList;

public class BootstrapTemplate {

  private static final Logger LOG =
    LoggerFactory.getLogger(BootstrapTemplate.class);

  public static Template build(
    ClusterSpec clusterSpec,
    ComputeService computeService,
    StatementBuilder statementBuilder,
    TemplateBuilderStrategy strategy
  ) throws MalformedURLException {

    LOG.info("Configuring template");

    Statement runScript = addUserAndAuthorizeSudo(
        clusterSpec.getClusterUser(),
        clusterSpec.getPublicKey(),
        clusterSpec.getPrivateKey(),
        statementBuilder.build(clusterSpec));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Running script:\n{}", runScript.render(OsFamily.UNIX));
    }

    TemplateBuilder templateBuilder = computeService.templateBuilder()
      .options(runScript(runScript));
    strategy.configureTemplateBuilder(clusterSpec, templateBuilder);

    return setSpotInstancePriceIfSpecified(
      computeService.getContext(), clusterSpec, templateBuilder.build()
    );
  }

  private static Statement addUserAndAuthorizeSudo(
    String user, String publicKey, String privateKey, Statement statement
  ) {
    return new InitBuilder(
      "setup-" + user,// name of the script
      "/tmp",// working directory
      "/tmp/logs",// location of stdout.log and stderr.log
      ImmutableMap.of("newUser", user, "defaultHome", "/home/users"), // variables
      ImmutableList.<Statement> of(
        createUserWithPublicAndPrivateKey(user, publicKey, privateKey),
        makeSudoersOnlyPermitting(user),
        statement)
    );
  }

  /**
   * Set maximum spot instance price based on the configuration
   */
  private static Template setSpotInstancePriceIfSpecified(
      ComputeServiceContext context, ClusterSpec spec, Template template) {

    if (context != null && context.getProviderSpecificContext().getApi() instanceof AWSEC2Client) {
      if (spec.getAwsEc2SpotPrice() > 0) {
        template.getOptions().as(AWSEC2TemplateOptions.class)
          .spotPrice(spec.getAwsEc2SpotPrice());
      }
    }

    return template;
  }

  // must be used inside InitBuilder, as this sets the shell variables used in this statement
  private static Statement createUserWithPublicAndPrivateKey(String username,
     String publicKey, String privateKey) {
    // note directory must be created first
    return newStatementList(
      interpret(
        "mkdir -p $DEFAULT_HOME/$NEW_USER/.ssh",
        "useradd --shell /bin/bash -d $DEFAULT_HOME/$NEW_USER $NEW_USER\n"),
      appendFile(
        "$DEFAULT_HOME/$NEW_USER/.ssh/authorized_keys",
        Splitter.on('\n').split(publicKey)),
      appendFile(
        "$DEFAULT_HOME/$NEW_USER/.ssh/id_rsa",
        Splitter.on('\n').split(privateKey)),
      interpret(
        "chmod 400 $DEFAULT_HOME/$NEW_USER/.ssh/*",
        "chown -R $NEW_USER $DEFAULT_HOME/$NEW_USER\n"));
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

}
