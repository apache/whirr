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

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Multimaps.synchronizedListMultimap;
import static com.google.common.io.ByteStreams.newInputStreamSupplier;
import static com.google.inject.matcher.Matchers.identicalTo;
import static com.google.inject.matcher.Matchers.returns;
import static com.google.inject.matcher.Matchers.subclassesOf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jclouds.compute.callables.RunScriptOnNode;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.crypto.CryptoStreams;
import org.jclouds.domain.Credentials;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.StringPayload;
import org.jclouds.net.IPSocket;
import org.jclouds.ssh.SshClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.inject.AbstractModule;

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
    private static final Logger LOG = LoggerFactory
            .getLogger(DryRunModule.class);

    // an example showing how to intercept any internal method for logging
    // purposes
    public class LogCallToRunScriptOnNode implements MethodInterceptor {

        public Object invoke(MethodInvocation i) throws Throwable {
            if (i.getMethod().getName().equals("call")) {
                RunScriptOnNode runScriptOnNode = RunScriptOnNode.class.cast(i
                        .getThis());
                String nodeName = runScriptOnNode.getNode().getName();
                LOG.info(nodeName + " >> running script");
                Object returnVal = i.proceed();
                LOG.info(nodeName + " << " + returnVal);
                return returnVal;
            } else {
                return i.proceed();
            }
        }
    }

    public static synchronized DryRun getDryRun() {
        return DryRun.INSTANCE;
    }

    public static void resetDryRun() {
        DryRun.INSTANCE.executedScripts.clear();
    }

    // enum singleton pattern
    public static enum DryRun {
        INSTANCE;
        // allow duplicate mappings and use deterministic ordering
        private final ListMultimap<NodeMetadata, RunScriptOnNode> executedScripts = synchronizedListMultimap(LinkedListMultimap
                .<NodeMetadata, RunScriptOnNode> create());

        DryRun() {
        }

        void newExecution(RunScriptOnNode runScript) {
            executedScripts.put(runScript.getNode(), runScript);
        }

        public synchronized ListMultimap<NodeMetadata, RunScriptOnNode> getExecutions() {
            return ImmutableListMultimap.copyOf(executedScripts);
        }

    }

    public class SaveDryRunsByInterceptingRunScriptOnNodeCreation implements
            MethodInterceptor {

        public Object invoke(MethodInvocation i) throws Throwable {
            if (i.getMethod().getName().equals("create")) {
                Object returnVal = i.proceed();
                getDryRun().newExecution(RunScriptOnNode.class.cast(returnVal));
                return returnVal;
            } else {
                return i.proceed();
            }
        }
    }

    @Override
    protected void configure() {
        bind(SshClient.Factory.class).to(LogSshClient.Factory.class);
        bindInterceptor(subclassesOf(RunScriptOnNode.class),
                returns(identicalTo(ExecResponse.class)),
                new LogCallToRunScriptOnNode());
        bindInterceptor(subclassesOf(RunScriptOnNode.Factory.class),
                returns(identicalTo(RunScriptOnNode.class)),
                new SaveDryRunsByInterceptingRunScriptOnNodeCreation());
    }

    private static class Key {
        private final IPSocket socket;
        private final Credentials creds;
        private final NodeMetadata node;

        Key(IPSocket socket, Credentials creds, @Nullable NodeMetadata node) {
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
                    socket.getAddress(), socket.getPort());
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
                        concat(arg0.getPrivateAddresses(),
                                arg0.getPublicAddresses()), address);
            }
        }

        @Singleton
        public static class Factory implements SshClient.Factory {

            // this will ensure only one state per ip socket/user
            private final Map<Key, SshClient> clientMap;
            // easy access to node metadata
            private final ConcurrentMap<String, NodeMetadata> nodes;

            @SuppressWarnings("unused")
            @Inject
            public Factory(final ConcurrentMap<String, NodeMetadata> nodes) {
                this.clientMap = new MapMaker()
                        .makeComputingMap(new Function<Key, SshClient>() {

                            @Override
                            public SshClient apply(Key key) {
                                return new LogSshClient(key);
                            }

                        });
                this.nodes = nodes;
            }

            @Override
            public SshClient create(final IPSocket socket,
                    Credentials loginCreds) {
                return clientMap.get(new Key(socket, loginCreds,
                        find(nodes.values(),
                                new NodeHasAddress(socket.getAddress()))));
            }

            @Override
            public SshClient create(IPSocket socket, String username,
                    String password) {
                return create(socket, new Credentials(username, password));
            }

            @Override
            public SshClient create(IPSocket socket, String username,
                    byte[] privateKey) {
                return create(socket, new Credentials(username, new String(
                        privateKey)));
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
            LOG.info(toString() + " << md5[" + md5Hex(returnVal) + "]");
            return returnVal;
        }

        @Override
        public String getHostAddress() {
            return key.socket.getAddress();
        }

        @Override
        public String getUsername() {
            return key.creds.identity;
        }

        @Override
        public void put(String path, Payload payload) {
            LOG.info(toString() + " >> put(" + path + ", md5["
                    + md5Hex(payload) + "])");
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
    }

    public static String md5Hex(String in) {
        return md5Hex(newInputStreamSupplier(in.getBytes()));
    }

    public static String md5Hex(InputSupplier<? extends InputStream> supplier) {
        try {
            return CryptoStreams.md5Hex(supplier);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
