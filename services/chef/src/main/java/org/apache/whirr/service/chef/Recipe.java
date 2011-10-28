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

package org.apache.whirr.service.chef;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Class representing a single Recipe. If the recipe is already in the cookbook
 * cache or if it is available at a the central repository there is no need to
 * provide an url.</p>
 * 
 * Typical usage is: <br/>
 * <blockquote>
 * 
 * <pre>
 * ...
 * 
 * @Override 
 * protected void beforeBootstrap(ClusterActionEvent event) throws
 *           IOException, InterruptedException {
 *           
 *     ...
 * 
 *     Recipe nginx = new Recipe("nginx", null, "http://example.com/nginx-bookbook.tgz");
 *     addStatement(event, nginx);
 *     ...
 *     OR
 *     
 *     Recipe java = new Recipe("java");
 *     addStatement(event,java);
 *     
 *     OR
 *     
 *     Recipe postgreServer = new Recipe("postgresql","server");
 *     addStatement(event, postgreServer);
 * 
 * }
 * 
 * ...
 * </pre>
 * 
 * </blockquote>
 * 
 */
public class Recipe implements Statement {

  public final String cookbook;
  public final String recipe;
  public String url;
  public final Map<String, Object> attribs = new LinkedHashMap<String, Object>();

  @VisibleForTesting
  String fileName = null;

  /**
   * To run the default recipe from a provided cookbook with the specified name.
   * 
   * @param cookbook
   */
  public Recipe(String cookbook) {
    this(cookbook, null);
  }

  /**
   * To run a particular recipe from a provided cookbook with the specified
   * name.
   * 
   * @param cookbook
   */
  public Recipe(String cookbook, String recipe) {
    this.cookbook = cookbook;
    this.recipe = recipe;
    fileName = cookbook + "::" + (recipe != null ? recipe : "default") + "-"
        + System.currentTimeMillis();
  }

  /**
   * To run a particular recipe from a cookbook located at <code>url</code>. To
   * use the default recipe pass <code>recipe</code> as <code>null</code>
   * 
   * @param cookbook
   * @param recipe
   * @param url
   */
  public Recipe(String cookbook, String recipe, String url) {
    this(cookbook, recipe);
    this.url = url;
  }

  /**
   * Package protected to be used only by {@link ChefClusterActionHandler}.
   * 
   * @param config
   */
  Recipe(String cookbook, String recipe, Configuration config) {
    this(cookbook, recipe);
    for (Iterator<?> iter = config.getKeys(); iter.hasNext();) {
      String key = (String) iter.next();
      String attribute = key.substring(cookbook.length() + 1, key.length());
      if (attribute.equals("url")) {
        this.url = config.getString(key);
      } else {
        attribs.put(attribute, config.getProperty(key));
      }
    }
  }

  /**
   * Transforms the recipe into a JSON format that can be interpreted by the
   * "chef-solo" command.
   * 
   * @return
   */
  public String toJSON() {
    Gson gson = new Gson();
    JsonObject recipeAsJSON = new JsonObject();

    if (!attribs.isEmpty()) {
      JsonObject jsonAttribs = new JsonObject();
      for (Map.Entry<String, Object> entry : attribs.entrySet()) {
        jsonAttribs.add(entry.getKey(), gson.toJsonTree(entry.getValue()));
      }

      // add the attribs
      recipeAsJSON.add(cookbook, jsonAttribs);
    }

    // add the recipe
    recipeAsJSON.add(
        "run_list",
        gson.toJsonTree(new String[] { "recipe[" + cookbook
            + (recipe != null ? ("::" + recipe) : "") + "]" }));

    return gson.toJson(recipeAsJSON);

  }

  @Override
  public Iterable<String> functionDependencies(OsFamily family) {
    // prob should use install_chef but not sure
    return Collections.emptyList();
  }

  /**
   * Transforms the recipe into the series of statements that must be ran on the
   * server (after chef is installed).
   * 
   * @return
   */
  @Override
  public String render(OsFamily family) {

    if (family.equals(OsFamily.UNIX)) {
      // store the json file in tmp
      String fileName = "/tmp/" + this.fileName + ".json";

      Statement storeJSonFile = Statements.createOrOverwriteFile(fileName,
          Collections.singleton(toJSON()));

      Statement runRecipe = null;

      runRecipe = Statements.exec("sudo -i chef-solo -j " + fileName
          + (url != null ? " -r " + url : ""));

      return Statements.newStatementList(storeJSonFile, runRecipe).render(
          family);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public static RunScriptOptions recipeScriptOptions(RunScriptOptions options) {
    return options.wrapInInitScript(true).runAsRoot(true);
  }

}
