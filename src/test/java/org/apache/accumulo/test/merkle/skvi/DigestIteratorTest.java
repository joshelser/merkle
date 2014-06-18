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
package org.apache.accumulo.test.merkle.skvi;

import java.io.File;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * 
 */
public class DigestIteratorTest {
  private static final File BASE_MAC_DIR = new File(System.getProperty("user.dir") + "/target/digest-test");
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
  public void test() throws Exception {
    TreeSet<Text> splits = new TreeSet<>();

    splits.add(new Text("5"));

    conn.tableOperations().addSplits(tableName, splits);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    for (int i = 0; i < 1000000; i++) {
      String value = Integer.toString(i);
      Mutation m = new Mutation(value);
      m.put(value, value, value);
      bw.addMutation(m);
    }
    bw.close();

    Scanner s = conn.createScanner(tableName, Authorizations.EMPTY);

    IteratorSetting cfg = new IteratorSetting(50, DigestIterator.class);
    cfg.addOption(DigestIterator.HASH_NAME_KEY, "MD5");

    s.addScanIterator(cfg);
    s.setRange(new Range());

    for (Entry<Key,Value> entry : s) {
      System.out.println(entry);
    }
  }

}
