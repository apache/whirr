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

package org.apache.whirr.service;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Multimaps.synchronizedListMultimap;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.io.BaseEncoding.base16;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jclouds.compute.domain.ExecChannel;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.events.StatementOnNode;
import org.jclouds.compute.events.StatementOnNodeSubmission;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.StringPayload;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.ssh.SshClient;
import org.jclouds.util.Strings2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

/**
 * Outputs orchestration jclouds does to INFO logging and saves an ordered list
 * of all scripts executed on the cluster that can be used to make assertions.
 * Use in tests by setting whirr.provider to "stub" and make sure you do not
 * specify a hardware, image, or location id.
 * 
 */
// note that most of this logic will be pulled into jclouds 1.0-beta-10 per
// http://code.google.com/p/jclouds/issues/detail?id=490
public class DryRunModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(DryRunModule.class);

  public static class DryRun {
    public DryRun reset() {
      executedScripts.clear();
      totallyOrderedScripts.clear();
      return this;
    }

    // stores the scripts executed, per node, in the order they were executed
    private final ListMultimap<NodeMetadata, Statement> executedScripts = synchronizedListMultimap(LinkedListMultimap
        .<NodeMetadata, Statement> create());
    
    private final List<StatementOnNode> totallyOrderedScripts = Collections.synchronizedList(new ArrayList<StatementOnNode>());

    DryRun() {
    }
       
    @Subscribe
    public void newExecution(StatementOnNodeSubmission info) {
      executedScripts.put(info.getNode(), info.getStatement());
      totallyOrderedScripts.add(info);
    }

    public synchronized ListMultimap<NodeMetadata, Statement> getExecutions() {
      return ImmutableListMultimap.copyOf(executedScripts);
    }
    
    public synchronized List<StatementOnNode> getTotallyOrderedExecutions() {
      return ImmutableList.copyOf(totallyOrderedScripts);
    }

  }

  @Override
  protected void configure() {
    bind(SshClient.Factory.class).to(LogSshClient.Factory.class);
  }
  
  @Singleton
  @Provides
  DryRun provideDryRun(EventBus in){
     DryRun dryRun = new DryRun();
     in.register(dryRun);
     return dryRun;
  }
  
  private static class Key {
    private final HostAndPort socket;
    private final Credentials creds;
    private final NodeMetadata node;

    Key(HostAndPort socket, Credentials creds, NodeMetadata node) {
      this.socket = socket;
      this.creds = creds;
      this.node = node;
    }

    // only the user, not password should be used to identify this
    // connection
    @Override
    public int hashCode() {
      return Objects.hashCode(socket, creds.identity);
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      return Objects.equal(this.toString(), that.toString());
    }

    @Override
    public String toString() {
      return String.format("%s#%s@%s:%d", node.getName(), creds.identity,
          socket.getHostText(), socket.getPort());
    }
  }

  @Singleton
  private static class LogSshClient implements SshClient {
    private final Key key;

    public LogSshClient(Key key) {
      this.key = key;
    }

    private static class NodeHasAddress implements Predicate<NodeMetadata> {
      private final String address;

      private NodeHasAddress(String address) {
        this.address = address;
      }

      @Override
      public boolean apply(NodeMetadata arg0) {
        return contains(
            concat(arg0.getPrivateAddresses(), arg0.getPublicAddresses()),
            address);
      }
    }

    @Singleton
    public static class Factory implements SshClient.Factory {

      // this will ensure only one state per ip socket/user
      private final LoadingCache<Key, SshClient> clientMap;
      // easy access to node metadata
      private final ConcurrentMap<String, NodeMetadata> nodes;

      @SuppressWarnings("unused")
      @Inject
      public Factory(final ConcurrentMap<String, NodeMetadata> nodes) {
         this.clientMap = CacheBuilder.newBuilder().build(
               new CacheLoader<Key, SshClient>(){
              @Override
              public SshClient load(Key key) {
                return new LogSshClient(key);
              }

            });
        this.nodes = nodes;
      }

      public SshClient create(final HostAndPort socket, Credentials loginCreds) {
        return clientMap.getUnchecked(new Key(socket, loginCreds, find(nodes.values(),
            new NodeHasAddress(socket.getHostText()))));
      }

      @Override
      public SshClient create(HostAndPort socket, LoginCredentials credentials) {
        return clientMap.getUnchecked(new Key(socket, credentials, find(nodes.values(),
            new NodeHasAddress(socket.getHostText()))));
      }
    }

    private final Map<String, Payload> contents = Maps.newConcurrentMap();

    @Override
    public void connect() {
      LOG.info(toString() + " >> connect()");
    }

    @Override
    public void disconnect() {
      LOG.info(toString() + " >> disconnect()");
    }

    public ThreadLocal<AtomicInteger> delay = new ThreadLocal<AtomicInteger>();
    public static int callDelay = 5;

    @Override
    public ExecResponse exec(String script) {
      LOG.info(toString() + " >> exec(" + script + ")");
      // jclouds checks the status code, but only when seeing if a job
      // completed. to emulate real scripts all calls are delayed by
      // forcing
      // jclouds to call status multiple times (5 by default) before
      // returning exitCode 1.
      if (delay.get() == null) {
        delay.set(new AtomicInteger(0));
      }
      ExecResponse exec;
      if (script.endsWith(" status")) {
        if (delay.get().get() >= callDelay) {
          exec = new ExecResponse("", "", 1);
        } else {
          exec = new ExecResponse("", "", 0);
        }
      } else if (script.endsWith(" exitstatus")) {
        // show return code is 0
        exec = new ExecResponse("0", "", 0);
      } else {
        exec = new ExecResponse("", "", 0);
      }

      LOG.info(toString() + " << " + exec);

      delay.get().getAndIncrement();
      return exec;
    }

    @Override
    public Payload get(String path) {
      LOG.info(toString() + " >> get(" + path + ")");
      Payload returnVal = contents.get(path);
      try {
        LOG.info(toString() + " << md5[" + md5Hex(Strings2.toString(returnVal)) + "]");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return returnVal;
    }

    @Override
    public String getHostAddress() {
      return key.socket.getHostText();
    }

    @Override
    public String getUsername() {
      return key.creds.identity;
    }

    @Override
    public void put(String path, Payload payload) {
      try {
        LOG.info(toString() + " >> put(" + path + ", md5[" + md5Hex(Strings2.toString(payload))
               + "])");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      contents.put(path, payload);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null)
        return false;
      return toString().equals(obj.toString());
    }

    @Override
    public int hashCode() {
      return key.hashCode();
    }

    @Override
    public String toString() {
      return key.toString();
    }

    @Override
    public void put(String path, String text) {
      put(path, new StringPayload(text));
    }

   @Override
   public ExecChannel execChannel(String command) {
      throw new UnsupportedOperationException();
   }
  }

  public static String md5Hex(String in) {
    return base16().lowerCase().encode(md5().hashString(in, UTF_8).asBytes());
  }

}
