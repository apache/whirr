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

import static junit.framework.Assert.assertEquals;

import org.jclouds.scriptbuilder.domain.OsFamily;
import org.junit.Test;

public class RecipeTest {

  private static final String NGINX_JSON = "{\"run_list\":[\"recipe[nginx]\"]}";
  private static final String POSTGRES_JSON = "{\"run_list\":[\"recipe[postgresql::server]\"]}";
  private static final String RESOLVER_JSON = "{\"resolver\":{\"nameservers\":[\"10.0.0.1\"],\"search\":\"int.example.com\"},\"run_list\":[\"recipe[resolver]\"]}";
  private static final String CHEF_COMMAND = "cat > /tmp/test.json <<-'END_OF_JCLOUDS_FILE'\n"
      + "\t{\"nginx\":{\"webport\":\"90\"},\"run_list\":[\"recipe[nginx]\"]}\n"
      + "END_OF_JCLOUDS_FILE\n" + "sudo -i chef-solo -j /tmp/test.json -r http://myurl\n";

  @Test
  public void testJSONConversion() {

    Recipe nginx = new Recipe("nginx");

    assertEquals("JSON representation is incorrect.", NGINX_JSON,
        nginx.toJSON());
  }

  @Test
  public void testJSONConversionWithSpecificRecipe() {

    Recipe postgresql = new Recipe("postgresql", "server");

    assertEquals("JSON representation is incorrect.", POSTGRES_JSON,
        postgresql.toJSON());

  }

  @Test
  public void testJSONConversionWithAttribs() {

    // use the example recipe from
    // http://wiki.opscode.com/display/chef/Chef+Solo

    Recipe resolver = new Recipe("resolver");
    resolver.attribs.put("nameservers", new String[] { "10.0.0.1" });
    resolver.attribs.put("search", "int.example.com");

    assertEquals("JSON representation is incorrect.", RESOLVER_JSON,
        resolver.toJSON());

  }

  @Test
  public void testStatementGeneration() {

    Recipe recipe = new Recipe("nginx", null, "http://myurl");
    recipe.attribs.put("webport", "90");

    recipe.fileName = "test";

    assertEquals("Statement representation incorrect", CHEF_COMMAND,
        recipe.render(OsFamily.UNIX));
  }

}
