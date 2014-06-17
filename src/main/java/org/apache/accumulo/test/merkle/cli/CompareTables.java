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
package org.apache.accumulo.test.merkle.cli;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * Accepts a set of tables, computes the hashes for each, and prints the top-level hash for each table.
 * <p>
 * Will automatically create output tables for intermediate hashes instead of requiring their existence.
 * This will raise an exception when the table we want to use already exists.
 */
public class CompareTables {
  private static final Logger log = LoggerFactory.getLogger(CompareTables.class);

  public static class CompareTablesOpts extends ClientOpts {
    @Parameter(names={"--tables"}, description = "Tables to compare", variableArity=true)
    public List<String> tables;

    @Parameter(names = {"-nt", "--numThreads"}, required = false, description = "number of concurrent threads calculating digests")
    private int numThreads = 4;

    @Parameter(names = {"-hash", "--hash"}, required = true, description = "type of hash to use")
    private String hashName;

    public List<String> getTables() {
      return this.tables;
    }

    public void setTables(List<String> tables) {
      this.tables = tables;
    }

    public int getNumThreads() {
      return numThreads;
    }

    public void setNumThreads(int numThreads) {
      this.numThreads = numThreads;
    }

    public String getHashName() {
      return hashName;
    }

    public void setHashName(String hashName) {
      this.hashName = hashName;
    }
  }

  private CompareTablesOpts opts;

  protected CompareTables() {}

  public CompareTables(CompareTablesOpts opts) {
    this.opts = opts;
  }

  public Map<String,String> computeAllHashes() throws AccumuloException, AccumuloSecurityException, TableExistsException, NoSuchAlgorithmException, TableNotFoundException {
    final Connector conn = opts.getConnector();
    final Map<String,String> hashesByTable = new HashMap<>();

    for (String table : opts.getTables()) {
      final String outputTableName = table + "_merkle";

      if (conn.tableOperations().exists(outputTableName)) {
        throw new IllegalArgumentException("Expected output table name to not yet exist: " + outputTableName);
      }

      conn.tableOperations().create(outputTableName);
      
      GenerateHashes genHashes = new GenerateHashes();
      try {
        genHashes.run(opts.getConnector(), table, table + "_merkle", opts.getHashName(), opts.getNumThreads());
      } catch (Exception e) {
        log.error("Error generating hashes for {}", table, e);
        throw new RuntimeException(e);
      }

      ComputeRootHash computeRootHash = new ComputeRootHash();
      String hash = Hex.encodeHexString(computeRootHash.getHash(conn, outputTableName, opts.getHashName()));

      hashesByTable.put(table, hash);
    }

    return hashesByTable;
  }

  public static void main(String[] args) throws Exception {
    CompareTablesOpts opts = new CompareTablesOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs("CompareTables", args, bwOpts);

    CompareTables compareTables = new CompareTables(opts);
    Map<String,String> tableToHashes = compareTables.computeAllHashes();

    for (Entry<String,String> entry : tableToHashes.entrySet()) {
      System.out.println(entry.getKey() + " " + entry.getValue());
    }
  }
}
