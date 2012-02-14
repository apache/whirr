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

package org.apache.whirr.command;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.PrintStream;
import java.util.EnumSet;
import java.util.Map;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.ClusterSpec.Property;
import static org.apache.whirr.ClusterSpec.Property.CLUSTER_NAME;
import static org.apache.whirr.ClusterSpec.Property.CREDENTIAL;
import static org.apache.whirr.ClusterSpec.Property.IDENTITY;
import static org.apache.whirr.ClusterSpec.Property.INSTANCE_TEMPLATES;
import static org.apache.whirr.ClusterSpec.Property.PRIVATE_KEY_FILE;
import static org.apache.whirr.ClusterSpec.Property.PROVIDER;
import org.apache.whirr.state.ClusterStateStore;
import org.apache.whirr.state.ClusterStateStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract command for interacting with clusters.
 */
public abstract class AbstractClusterCommand extends Command {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(AbstractClusterCommand.class);

  protected ClusterControllerFactory factory;
  protected ClusterStateStoreFactory stateStoreFactory;

  protected OptionParser parser = new OptionParser();
  private Map<Property, OptionSpec<?>> optionSpecs;
  private OptionSpec<String> configOption = parser
    .accepts("config", "Note that Whirr properties specified in " + 
      "this file  should all have a whirr. prefix.")
    .withRequiredArg()
    .describedAs("config.properties")
    .ofType(String.class);

  public AbstractClusterCommand(String name, String description, ClusterControllerFactory factory) {
    this(name, description, factory, new ClusterStateStoreFactory());
  }

  public AbstractClusterCommand(String name, String description, ClusterControllerFactory factory,
        ClusterStateStoreFactory stateStoreFactory) {
    super(name, description);

    this.factory = factory;
    this.stateStoreFactory = stateStoreFactory;

    optionSpecs = Maps.newHashMap();
    for (Property property : EnumSet.allOf(Property.class)) {
      ArgumentAcceptingOptionSpec<?> spec = parser
        .accepts(property.getSimpleName(), property.getDescription())
        .withRequiredArg()
        .ofType(property.getType());
      if (property.hasMultipleArguments()) {
        spec.withValuesSeparatedBy(',');
      }
      optionSpecs.put(property, spec);
    }
  }

  /**
   * Load the cluster spec by parsing the command line option set
   */
  protected ClusterSpec getClusterSpec(OptionSet optionSet) throws ConfigurationException {
    Configuration optionsConfig = new PropertiesConfiguration();
    for (Map.Entry<Property, OptionSpec<?>> entry : optionSpecs.entrySet()) {
      Property property = entry.getKey();
      OptionSpec<?> option = entry.getValue();
      Object value;
      if (property.hasMultipleArguments()) {
        value = optionSet.valuesOf(option);
      } else {
        value = optionSet.valueOf(option);
      }
      if (value != null) {
        optionsConfig.setProperty(property.getConfigName(), value);
      }
    }
    CompositeConfiguration config = new CompositeConfiguration();
    config.addConfiguration(optionsConfig);
    if (optionSet.has(configOption)) {
      Configuration defaults = new PropertiesConfiguration(optionSet.valueOf(configOption));
      config.addConfiguration(defaults);
    }
    ClusterSpec clusterSpec = new ClusterSpec(config);

    for (Property required : EnumSet.of(CLUSTER_NAME, PROVIDER, IDENTITY, CREDENTIAL,
        INSTANCE_TEMPLATES, PRIVATE_KEY_FILE)) {
      if (clusterSpec.getConfiguration().getString(required.getConfigName()) == null) {
        throw new IllegalArgumentException(String.format("Option '%s' not set.",
            required.getSimpleName()));
      }
    }

    return clusterSpec;
  }

  /**
   * Get the cluster instance together with NodeMetadata (through API calls)
   */
  protected Cluster getCluster(ClusterSpec clusterSpec, ClusterController controller)
      throws IOException, InterruptedException {
    return new Cluster(controller.getInstances(
        clusterSpec, createClusterStateStore(clusterSpec)));
  }

  /**
   * Create the specified service
   */
  protected ClusterController createClusterController(String serviceName) {
    ClusterController controller = factory.create(serviceName);
    if (controller == null) {
      LOG.warn("Unable to find service {}, using default.", serviceName);
      controller = factory.create(null);
    }
    return controller;
  }

  /**
   * Create the cluster state store object
   */
  protected ClusterStateStore createClusterStateStore(ClusterSpec spec) {
    return stateStoreFactory.create(spec);
  }

  /**
   * Print command execution error and a hint to help the user get more help
   */
  protected void printErrorAndHelpHint(PrintStream stream, Throwable e) {
    stream.println(e.getMessage());
    stream.println("Help: whirr help " + getName());
  }

  /**
   * Print a generic usage indication for commands
   */
  @Override
  public void printUsage(PrintStream stream) throws IOException {
    stream.println("Usage: whirr " + getName() + " [OPTIONS]");
    stream.println();
    parser.printHelpOn(stream);
  }
}
