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

package org.apache.whirr.cli;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

import org.apache.whirr.command.Command;
import org.junit.Test;

public class MainTest {
  
  static class TestCommand extends Command {
    public TestCommand() {
      super("test-command", "test description");
    }

    @Override
    public int run(InputStream in, PrintStream out, PrintStream err,
        List<String> args) throws Exception {
      return 0;
    }
  }
  
  @Test
  public void testNoArgs() throws Exception {
    Main main = new Main(new TestCommand());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bytes);
    int rc = main.run(null, out, null, Collections.<String>emptyList());
    assertThat(rc, is(-1));
    assertThat(bytes.toString(), containsString("Usage: whirr COMMAND"));
    assertThat(bytes.toString(), containsString("test-command  test description"));
  }
  
  @Test
  public void testUnrecognizedCommand() throws Exception {
    Main main = new Main(new TestCommand());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(bytes);
    int rc = main.run(null, null, err, Lists.newArrayList("bogus-command"));
    assertThat(rc, is(-1));
    assertThat(bytes.toString(), containsString("Unrecognized command 'bogus-command'"));
  }
  
  @Test
  public void testCommand() throws Exception {
    Command command = mock(Command.class);
    when(command.getName()).thenReturn("mock-command");
    Main main = new Main(command);
    main.run(null, null, null, Lists.newArrayList("mock-command", "arg1", "arg2"));
    verify(command).run(null, null, null, Lists.newArrayList("arg1", "arg2"));
  }
}
