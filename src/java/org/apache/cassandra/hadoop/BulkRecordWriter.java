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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.net.UnknownHostException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.hadoop.util.Progressable;

public final class BulkRecordWriter extends AbstractBulkRecordWriter<ByteBuffer, List<Mutation>>
{
    private final Logger logger = LoggerFactory.getLogger(BulkRecordWriter.class);
    private File outputDir;
    
    
    private enum CFType
    {
        NORMAL,
        SUPER,
    }

    private enum ColType
    {
        NORMAL,
        COUNTER
    }

    private CFType cfType;
    private ColType colType;

    BulkRecordWriter(TaskAttemptContext context)
    {
        super(context);
    }

    BulkRecordWriter(Configuration conf, Progressable progress)
    {
        super(conf, progress);
    }

    BulkRecordWriter(Configuration conf)
    {
        super(conf);
    }

    private void setTypes(Mutation mutation)
    {
       if (cfType == null)
       {
           if (mutation.getColumn_or_supercolumn().isSetSuper_column() || mutation.getColumn_or_supercolumn().isSetCounter_super_column())
               cfType = CFType.SUPER;
           else
               cfType = CFType.NORMAL;
           if (mutation.getColumn_or_supercolumn().isSetCounter_column() || mutation.getColumn_or_supercolumn().isSetCounter_super_column())
               colType = ColType.COUNTER;
           else
               colType = ColType.NORMAL;
       }
    }

    private void prepareWriter() throws IOException
    {
        if (outputDir == null)
        {
            String keyspace = ConfigHelper.getOutputKeyspace(conf);
            //dir must be named by ks/cf for the loader
            outputDir = new File(getOutputLocation() + File.separator + keyspace + File.separator + ConfigHelper.getOutputColumnFamily(conf));
            outputDir.mkdirs();
        }
        
        if (writer == null)
        {
            AbstractType<?> subcomparator = null;
            ExternalClient externalClient = null;
            String username = ConfigHelper.getOutputKeyspaceUserName(conf);
            String password = ConfigHelper.getOutputKeyspacePassword(conf);

            if (cfType == CFType.SUPER)
                subcomparator = BytesType.instance;

            writer = new SSTableSimpleUnsortedWriter(
                    outputDir,
                    ConfigHelper.getOutputPartitioner(conf),
                    ConfigHelper.getOutputKeyspace(conf),
                    ConfigHelper.getOutputColumnFamily(conf),
                    BytesType.instance,
                    subcomparator,
                    Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64")),
                    ConfigHelper.getOutputCompressionParamaters(conf));

            externalClient = new ExternalClient(ConfigHelper.getOutputInitialAddress(conf),
                                                ConfigHelper.getOutputRpcPort(conf),
                                                username,
                                                password);

            this.loader = new SSTableLoader(outputDir, externalClient, new NullOutputHandler());
        }
    }

    @Override
    public void write(ByteBuffer keybuff, List<Mutation> value) throws IOException
    {
        setTypes(value.get(0));
        prepareWriter();
        SSTableSimpleUnsortedWriter ssWriter = (SSTableSimpleUnsortedWriter) writer;
        ssWriter.newRow(keybuff);
        for (Mutation mut : value)
        {
            if (cfType == CFType.SUPER)
            {
                ssWriter.newSuperColumn(mut.getColumn_or_supercolumn().getSuper_column().name);
                if (colType == ColType.COUNTER)
                    for (CounterColumn column : mut.getColumn_or_supercolumn().getCounter_super_column().columns)
                        ssWriter.addCounterColumn(column.name, column.value);
                else
                {
                    for (Column column : mut.getColumn_or_supercolumn().getSuper_column().columns)
                    {
                        if(column.ttl == 0)
                            ssWriter.addColumn(column.name, column.value, column.timestamp);
                        else
                            ssWriter.addExpiringColumn(column.name, column.value, column.timestamp, column.ttl, System.currentTimeMillis() + ((long)column.ttl * 1000));
                    }
                }
            }
            else
            {
                if (colType == ColType.COUNTER)
                    ssWriter.addCounterColumn(mut.getColumn_or_supercolumn().counter_column.name, mut.getColumn_or_supercolumn().counter_column.value);
                else
                {
                    if(mut.getColumn_or_supercolumn().column.ttl == 0)
                        ssWriter.addColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp);
                    else
                        ssWriter.addExpiringColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp, mut.getColumn_or_supercolumn().column.ttl, System.currentTimeMillis() + ((long)(mut.getColumn_or_supercolumn().column.ttl) * 1000));
                }
            }
            if (null != progress)
                progress.progress();
            if (null != context)
                HadoopCompat.progress(context);
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
}
