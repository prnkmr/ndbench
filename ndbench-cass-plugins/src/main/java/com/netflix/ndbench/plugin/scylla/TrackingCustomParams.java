/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ndbench.plugin.scylla;

import com.datastax.driver.core.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import com.netflix.ndbench.plugin.cass.CassJavaDriverManager;
import com.netflix.ndbench.plugin.cass.CassJavaDriverPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * This is a Elassandra(http://www.elassandra.io/) plugin using the CASS api. <BR> 
 * You need make sure you install Elassandra properly on top of your Cass database.
 * More details on elassandra installation here: http://doc.elassandra.io/en/latest/installation.html
 * 
 * This plugin will create the schema and will stress test Elassandra via Datastax driver.
 * 
 * @author diegopacheco
 *
 */
@Singleton
@NdBenchClientPlugin("TrackingCustomParams")
public class TrackingCustomParams implements NdBenchClient{
    private static final Logger Logger = LoggerFactory.getLogger(TrackingCustomParams.class);

    private Cluster cluster;
    private Session session;

    private DataGenerator dataGenerator;
    protected PropertyFactory propertyFactory;

    private String ClusterName = "email", ClusterContactPoint ="127.0.0.1", KeyspaceName ="email", TableName ="tracking_custom_params";
    //private String ClusterName = "Test Cluster", ClusterContactPoint ="172.28.198.16", KeyspaceName ="customer", TableName ="external";
        
    private ConsistencyLevel WriteConsistencyLevel=ConsistencyLevel.TWO, ReadConsistencyLevel=ConsistencyLevel.LOCAL_ONE;

    private PreparedStatement readPstmt;
    private PreparedStatement writePstmt;

    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;


    /**
     * Initialize the client
     *
     * @throws Exception
     */
    
    @Inject
    public TrackingCustomParams(PropertyFactory propertyFactory) {
        this.propertyFactory = propertyFactory;
    }
    
    public void init(DataGenerator dataGenerator) throws Exception {
    	
    	ClusterName = propertyFactory.getProperty(NdBenchConstants.PROP_PREFIX+"cass.cluster").asString("email").get();
        ClusterContactPoint = propertyFactory.getProperty(NdBenchConstants.PROP_PREFIX+"cass.host").asString("127.0.0.1").get();
        KeyspaceName = propertyFactory.getProperty(NdBenchConstants.PROP_PREFIX+"cass.keyspace").asString("email").get();
        
        Logger.info("Cassandra  Cluster: " + ClusterName);
        this.dataGenerator = dataGenerator;
        cluster = Cluster.builder()
                .withClusterName("email")
                .addContactPoint(ClusterContactPoint)
                .build();
        session = cluster.connect();

        upsertKeyspace(this.session);
        upsertCF(this.session);

        writePstmt = session.prepare("INSERT INTO "+ TableName +" (id , app_id , from_addr , to_addr , params , urls  ) VALUES (?, ?, ?, ?, ?, ?)");
        readPstmt = session.prepare("SELECT * From "+ TableName +" Where id = ?");

        Logger.info("Initialized TrackingCustomParams");
    }

    /**
     * Perform a single read operation
     *
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    public String readSingle(String key) throws Exception {
        BoundStatement bStmt = readPstmt.bind();
        bStmt.setString("id", key);
        bStmt.setConsistencyLevel(this.ReadConsistencyLevel);
        ResultSet rs = session.execute(bStmt);

        List<Row> result=rs.all();
        if (!result.isEmpty())
        {
            if (result.size() != 1) {
                throw new Exception("Num Cols returned not ok " + result.size());
            }
        }
        else {
            return CacheMiss;
        }

        return ResultOK;
    }

    /**
     * Perform a single write operation
     *
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    public String writeSingle(String key) throws Exception {
        BoundStatement bStmt = writePstmt.bind();
        bStmt.setString("id", key);
        bStmt.setVarint("app_id", new BigInteger(dataGenerator.getRandomIntegerValue().toString()));
        bStmt.setString("from_addr"	, this.dataGenerator.getRandomString());
        bStmt.setString("params"	, this.dataGenerator.getRandomString());
        bStmt.setString("to_addr"	, this.dataGenerator.getRandomString());
        bStmt.setString("from_addr"	, this.dataGenerator.getRandomString());
        bStmt.setString("urls", this.dataGenerator.getRandomString()) ;
        bStmt.setConsistencyLevel(this.WriteConsistencyLevel);

        session.execute(bStmt);
        return ResultOK;
    }

    /**
     * shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        Logger.info("Shutting down TrackingCustomParams");
        cluster.close();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("Cluster Name - %s : Keyspace Name - %s : CF Name - %s ::: ReadCL - %s : WriteCL - %s ", ClusterName, KeyspaceName, TableName, ReadConsistencyLevel, WriteConsistencyLevel);
    }

    /**
     * Run workflow for functional testing
     *
     * @throws Exception
     */
    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }

    void upsertKeyspace(Session session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KeyspaceName +" WITH replication = {'class':'SimpleStrategy','replication_factor':2} ;");
        //session.execute("CREATE KEYSPACE IF NOT EXISTS " + KeyspaceName +" WITH replication = {'class':'SimpleStrategy','replication_factor': 2};");
        session.execute("Use " + KeyspaceName);
    }
    
    void upsertCF(Session session) {
        session.execute("CREATE TABLE IF NOT EXISTS "+ TableName +" (id text primary key, app_id varint, from_addr text, to_addr text, params text, urls text)");
        
    }
}
