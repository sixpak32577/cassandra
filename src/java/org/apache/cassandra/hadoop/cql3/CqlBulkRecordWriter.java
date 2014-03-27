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
package org.apache.cassandra.hadoop.cql3;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.hadoop.AbstractBulkRecordWriter;
import org.apache.cassandra.hadoop.BulkRecordWriter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

/**
 * The <code>CqlBulkRecordWriter</code> maps the output &lt;key, value&gt;
 * pairs to a Cassandra column family. In particular, it applies the binded variables
 * in the value to the prepared statement, which it associates with the key, and in 
 * turn the responsible endpoint.
 *
 * <p>
 * Furthermore, this writer groups the cql queries by the endpoint responsible for
 * the rows being affected. This allows the cql queries to be executed in parallel,
 * directly to a responsible endpoint.
 * </p>
 *
 * @see CqlBulkOutputFormat
 */
class CqlBulkRecordWriter extends AbstractBulkRecordWriter<List<ByteBuffer>, List<ByteBuffer>>
{
    private static final String COLUMNFAMILY_SCHEMA = "mapreduce.output.bulkoutputformat.columnfamily.schema";
    private static final String COLUMNFAMILY_INSERT_STATEMENT = "mapreduce.output.bulkoutputformat.columnfamily.insert.statement";
    
    private String keyspace;
    private String columnFamily;


    CqlBulkRecordWriter(TaskAttemptContext context) throws IOException
    {
        super(context);
        setConfigs();
    }

    CqlBulkRecordWriter(Configuration conf, Progressable progress) throws IOException
    {
        super(conf, progress);
        setConfigs();
    }

    CqlBulkRecordWriter(Configuration conf)
    {
        super(conf);
        setConfigs();
    }
    
    private void setConfigs()
    {
        keyspace = ConfigHelper.getOutputKeyspace(conf);
        columnFamily = ConfigHelper.getOutputColumnFamily(conf);
    }

    
    private void prepareWriter() throws IOException
    {        
        File outputDir = getColumnFamilyDirectory();
        String schema = getColumnFamilySchema();
        try {
            if (writer == null)
            {
                writer = CQLSSTableWriter.builder()
                    .forTable(schema)
                    .using(getColumnFamilyInsertStatement())
                    .withPartitioner(ConfigHelper.getOutputPartitioner(conf))
                    .inDirectory(outputDir)
                    .withBufferSizeInMB(Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64")))
                    .build();
            }
            if (loader == null)
            {
                ExternalClient externalClient = new ExternalClient(
                    ConfigHelper.getOutputInitialAddress(conf),
                    ConfigHelper.getOutputRpcPort(conf),
                    ConfigHelper.getOutputKeyspaceUserName(conf),
                    ConfigHelper.getOutputKeyspacePassword(conf),
                    columnFamily, schema);

                this.loader = new SSTableLoader(outputDir, externalClient, new BulkRecordWriter.NullOutputHandler());
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }      
    }
    
    /**
     * The order of key and column values must correspond to the order in which
     * they appear in the insert stored procedure.
     * </p>
     *
     * @param keyColumns
     *            the key to write.
     * @param values
     *            the values to write.
     * @throws IOException
     */
    @Override
    public void write(List<ByteBuffer> keyColumns, List<ByteBuffer> values) throws IOException
    {
        prepareWriter();
        try {
            keyColumns.addAll(values);
            ((CQLSSTableWriter) writer).rawAddRow(keyColumns);
        } catch (org.apache.cassandra.exceptions.InvalidRequestException e) {
            throw new IOException("Error adding row", e);
        }
    }
  
    
    private String getColumnFamilySchema()
    {
        return conf.get(COLUMNFAMILY_SCHEMA + "." + columnFamily);
    }
    
    private String getColumnFamilyInsertStatement()
    {
        return conf.get(COLUMNFAMILY_INSERT_STATEMENT + "." + columnFamily);
    }
    
    private File getColumnFamilyDirectory() throws IOException
    {
        return new File(getOutputLocation() + File.separator + keyspace + File.separator + columnFamily);
    }
    
    
    public static class ExternalClient extends BulkRecordWriter.ExternalClient
    {
        String columnFamily;
        String cql;
      
        public ExternalClient(String hostlist, int port, String username, String password, String columnFamily, String cql)
        {
            super(hostlist, port, username, password);
            this.cql = cql;
        }
        
        @Override
        public CFMetaData getCFMetaData(String keyspace, String cfName)
        {
            CFMetaData metadata = super.getCFMetaData(keyspace, cfName);
            if (metadata != null)
            {
                return metadata;
            }
            
            if (columnFamily.equals(cfName))
            {
                return CFMetaData.compile(cql, keyspace);
            }
            
            return null;
        }
    }
}
