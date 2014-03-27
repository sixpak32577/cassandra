/*
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
package org.apache.cassandra.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.hadoop.util.Progressable;

public abstract class AbstractBulkRecordWriter<K, V> extends RecordWriter<K, V>
implements org.apache.hadoop.mapred.RecordWriter<K, V>
{
    protected final static String OUTPUT_LOCATION = "mapreduce.output.bulkoutputformat.localdir";
    protected final static String BUFFER_SIZE_IN_MB = "mapreduce.output.bulkoutputformat.buffersize";
    protected final static String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";
    protected final static String MAX_FAILED_HOSTS = "mapreduce.output.bulkoutputformat.maxfailedhosts";
    
    private final Logger logger = LoggerFactory.getLogger(AbstractBulkRecordWriter.class);
    
    protected final Configuration conf;
    protected final int maxFailures;
    protected final int bufferSize; 
    protected Closeable writer;
    protected SSTableLoader loader;
    protected Progressable progress;
    protected TaskAttemptContext context;
    
    protected AbstractBulkRecordWriter(TaskAttemptContext context)
    {
        this(HadoopCompat.getConfiguration(context));
        this.context = context;
    }

    protected AbstractBulkRecordWriter(Configuration conf, Progressable progress)
    {
        this(conf);
        this.progress = progress;
    }

    protected AbstractBulkRecordWriter(Configuration conf)
    {
        Config.setClientMode(true);
        Config.setOutboundBindAny(true);
        this.conf = conf;
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(Integer.parseInt(conf.get(STREAM_THROTTLE_MBITS, "0")));
        maxFailures = Integer.parseInt(conf.get(MAX_FAILED_HOSTS, "0"));
        bufferSize = Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64"));
    }

    protected String getOutputLocation() throws IOException
    {
        String dir = conf.get(OUTPUT_LOCATION, System.getProperty("java.io.tmpdir"));
        if (dir == null)
            throw new IOException("Output directory not defined, if hadoop is not setting java.io.tmpdir then define " + OUTPUT_LOCATION);
        return dir;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException
    {
        close();
    }

    /** Fills the deprecated RecordWriter interface for streaming. */
    @Deprecated
    public void close(org.apache.hadoop.mapred.Reporter reporter) throws IOException
    {
        close();
    }

    private void close() throws IOException
    {
        if (writer != null)
        {
            writer.close();
            Future<StreamState> future = loader.stream();
            while (true)
            {
                try
                {
                    future.get(1000, TimeUnit.MILLISECONDS);
                    break;
                }
                catch (ExecutionException | TimeoutException te)
                {
                    if (null != progress)
                        progress.progress();
                    if (null != context)
                        HadoopCompat.progress(context);
                }
                catch (InterruptedException e)
                {
                    throw new IOException(e);
                }
            }
            if (loader.getFailedHosts().size() > 0)
            {
                if (loader.getFailedHosts().size() > maxFailures)
                    throw new IOException("Too many hosts failed: " + loader.getFailedHosts());
                else
                    logger.warn("Some hosts failed: {}", loader.getFailedHosts());
            }
        }
    }

    public static class ExternalClient extends SSTableLoader.Client
    {
        private final Map<String, Map<String, CFMetaData>> knownCfs = new HashMap<>();
        private final String hostlist;
        private final int rpcPort;
        private final String username;
        private final String password;

        public ExternalClient(String hostlist, int port, String username, String password)
        {
            super();
            this.hostlist = hostlist;
            this.rpcPort = port;
            this.username = username;
            this.password = password;
        }

        public void init(String keyspace)
        {
            Set<InetAddress> hosts = new HashSet<InetAddress>();
            String[] nodes = hostlist.split(",");
            for (String node : nodes)
            {
                try
                {
                    hosts.add(InetAddress.getByName(node));
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }
            Iterator<InetAddress> hostiter = hosts.iterator();
            while (hostiter.hasNext())
            {
                try
                {
                    InetAddress host = hostiter.next();
                    Cassandra.Client client = createThriftClient(host.getHostAddress(), rpcPort);

                    // log in
                    client.set_keyspace(keyspace);
                    if (username != null)
                    {
                        Map<String, String> creds = new HashMap<String, String>();
                        creds.put(IAuthenticator.USERNAME_KEY, username);
                        creds.put(IAuthenticator.PASSWORD_KEY, password);
                        AuthenticationRequest authRequest = new AuthenticationRequest(creds);
                        client.login(authRequest);
                    }

                    List<TokenRange> tokenRanges = client.describe_ring(keyspace);
                    List<KsDef> ksDefs = client.describe_keyspaces();

                    setPartitioner(client.describe_partitioner());
                    Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

                    for (TokenRange tr : tokenRanges)
                    {
                        Range<Token> range = new Range<Token>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
                        for (String ep : tr.endpoints)
                        {
                            addRangeForEndpoint(range, InetAddress.getByName(ep));
                        }
                    }

                    for (KsDef ksDef : ksDefs)
                    {
                        Map<String, CFMetaData> cfs = new HashMap<>(ksDef.cf_defs.size());
                        for (CfDef cfDef : ksDef.cf_defs)
                            cfs.put(cfDef.name, CFMetaData.fromThrift(cfDef));
                        knownCfs.put(ksDef.name, cfs);
                    }
                    break;
                }
                catch (Exception e)
                {
                    if (!hostiter.hasNext())
                        throw new RuntimeException("Could not retrieve endpoint ranges: ", e);
                }
            }
        }

        public CFMetaData getCFMetaData(String keyspace, String cfName)
        {
            Map<String, CFMetaData> cfs = knownCfs.get(keyspace);
            return cfs != null ? cfs.get(cfName) : null;
        }

        private static Cassandra.Client createThriftClient(String host, int port) throws TTransportException
        {
            TSocket socket = new TSocket(host, port);
            TTransport trans = new TFramedTransport(socket);
            trans.open();
            TProtocol protocol = new org.apache.thrift.protocol.TBinaryProtocol(trans);
            return new Cassandra.Client(protocol);
        }
    }

    public static class NullOutputHandler implements OutputHandler
    {
        public void output(String msg) {}
        public void debug(String msg) {}
        public void warn(String msg) {}
        public void warn(String msg, Throwable th) {}
    }
}
