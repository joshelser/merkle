/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.merkle;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ClientOpts.Password;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.test.merkle.GenerateHashes.GenerateHashesOpts;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * 
 */
public class GenerateHashesMacTest {
  private static final File BASE_MAC_DIR = new File(System.getProperty("user.dir") + "/target/minicluster");
  private static final String PASSWORD = "password";

  private static MiniAccumuloConfig cfg;
  private static MiniAccumuloCluster cluster;

  @Rule
  public TestName test = new TestName();

  protected Connector conn;
  protected String tableName;

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    if (BASE_MAC_DIR.exists()) {
      FileUtils.deleteDirectory(BASE_MAC_DIR);
    }

    BASE_MAC_DIR.mkdirs();

    cfg = new MiniAccumuloConfig(BASE_MAC_DIR, PASSWORD);
    cfg.setNumTservers(2);
    cluster = new MiniAccumuloCluster(cfg);

    cluster.start();
  }

  @AfterClass
  public static void stopMiniCluster() throws Exception {
    if (null != cluster) {
      cluster.stop();
    }
  }

  @Before
  public void setupConnector() throws Exception {
    tableName = test.getMethodName();
    conn = cluster.getConnector("root", PASSWORD);
    conn.tableOperations().create(tableName);
  }

  @After
  public void removeTestTable() throws Exception {
    if (conn.tableOperations().exists(tableName)) {
      conn.tableOperations().delete(tableName);
    }
  }

  @Test
  public void simpleTest() throws Exception {
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    for (int i = 0; i < 5; i++) {
      Mutation m = new Mutation(Integer.toString(i));
      m.put("cf", "cq", "val");
      bw.addMutation(m);
    }

    bw.close();

    final String outputTable = "hashes";

    TreeSet<Text> splits = new TreeSet<>();
    splits.add(new Text("1"));
    splits.add(new Text("2"));
    splits.add(new Text("3"));
    splits.add(new Text("4"));

    conn.tableOperations().addSplits(tableName, splits);

    GenerateHashesOpts opts = new GenerateHashesOpts();
    opts.auths = new Authorizations();
    opts.instance = cluster.getInstanceName();
    opts.zookeepers = cluster.getZooKeepers();
    opts.principal = "root";
    opts.tokenClassName = PasswordToken.class.getName();
    opts.password = new Password(PASSWORD);
    opts.setHashName("MD5");
    opts.setNumThreads(2);
    opts.setOutputTableName(outputTable);
    opts.setTableName(tableName);

    conn.tableOperations().create(outputTable);

    GenerateHashes generate = new GenerateHashes(opts);
    generate.run();

    List<Key> expectedKeys = new ArrayList<>(Arrays.asList(new Key("", "", "1\0"), new Key("1\0", "", "2\0"), new Key("2\0", "", "3\0"), new Key("3\0", "", "4\0"), new Key(
        "4\0", "", "")));

    Scanner s = conn.createScanner(outputTable, new Authorizations());
    for (Entry<Key,Value> entry : s) {
      Key expectedKey = expectedKeys.remove(0);
      Assert.assertEquals(0, expectedKey.compareTo(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    }

    Assert.assertEquals("Expected to find more keys in the hashes table: " + expectedKeys, 0, expectedKeys.size());
  }

}
