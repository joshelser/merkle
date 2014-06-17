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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Range;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class MerkleTreeTest {

  @Test
  public void baseRollup() throws Exception {
    List<MerkleTreeNode> leaves = new ArrayList<>();

    byte[] hashOne = "hashone".getBytes(StandardCharsets.UTF_8), hashTwo = "hashtwo".getBytes(StandardCharsets.UTF_8);

    leaves.add(new MerkleTreeNode(new Range(null, false, "5", false), 0, Collections.<Range> emptyList(), hashOne));
    leaves.add(new MerkleTreeNode(new Range("5", true, null, false), 0, Collections.<Range> emptyList(), hashTwo));

    MerkleTree tree = new MerkleTree(leaves, "MD5");
    MerkleTreeNode root = tree.getRootNode();

    MessageDigest md5 = MessageDigest.getInstance("MD5");
    md5.update(hashOne);
    md5.update(hashTwo);
    byte[] rootHash = md5.digest();

    Assert.assertEquals(1, root.getLevel());
    Assert.assertArrayEquals(rootHash, root.getHash());
    Assert.assertEquals(Arrays.asList(new Range(null, false, "5", false), new Range("5", true, null, false)), root.getChildren());
    Assert.assertEquals(new Range(), root.getRange());
  }

  @Test
  public void identityRollup() throws Exception {
    List<MerkleTreeNode> leaves = new ArrayList<>();

    byte[] hashOne = "hashone".getBytes(StandardCharsets.UTF_8);

    leaves.add(new MerkleTreeNode(new Range(), 0, Collections.<Range> emptyList(), hashOne));

    MerkleTree tree = new MerkleTree(leaves, "MD5");
    MerkleTreeNode root = tree.getRootNode();

    Assert.assertEquals(leaves.get(0), root);
  }

  @Test
  public void oddLeavesRollup() throws Exception {
    List<MerkleTreeNode> leaves = new ArrayList<>();

    byte[] hashOne = "hashone".getBytes(StandardCharsets.UTF_8), hashTwo = "hashtwo".getBytes(StandardCharsets.UTF_8), hashThree = "hashthree"
        .getBytes(StandardCharsets.UTF_8), hashFour = "hashfour".getBytes(StandardCharsets.UTF_8), hashFive = "hashfive".getBytes(StandardCharsets.UTF_8);

    leaves.add(new MerkleTreeNode(new Range(null, false, "1", false), 0, Collections.<Range> emptyList(), hashOne));
    leaves.add(new MerkleTreeNode(new Range("1", true, "2", false), 0, Collections.<Range> emptyList(), hashTwo));
    leaves.add(new MerkleTreeNode(new Range("2", true, "3", false), 0, Collections.<Range> emptyList(), hashThree));
    leaves.add(new MerkleTreeNode(new Range("3", true, "4", false), 0, Collections.<Range> emptyList(), hashFour));
    leaves.add(new MerkleTreeNode(new Range("4", true, null, false), 0, Collections.<Range> emptyList(), hashFive));

    MerkleTree tree = new MerkleTree(leaves, "MD5");
    MerkleTreeNode root = tree.getRootNode();

    MessageDigest md5 = MessageDigest.getInstance("MD5");
    md5.update(hashOne);
    md5.update(hashTwo);
    byte[] hashOneTwo = md5.digest();

    md5.reset();
    md5.update(hashThree);
    md5.update(hashFour);
    byte[] hashThreeFour = md5.digest();

    md5.reset();
    md5.update(hashOneTwo);
    md5.update(hashThreeFour);
    byte[] hashOneFour = md5.digest();

    md5.reset();
    md5.update(hashOneFour);
    md5.update(hashFive);
    byte[] rootHash = md5.digest();

    Assert.assertEquals(3, root.getLevel());
    Assert.assertArrayEquals(rootHash, root.getHash());
    Assert.assertEquals(Arrays.asList(new Range(null, false, "4", false), new Range("4", true, null, false)), root.getChildren());
    Assert.assertEquals(new Range(), root.getRange());
  }

  @Test
  public void balancedLeavesRollup() throws Exception {
    List<MerkleTreeNode> leaves = new ArrayList<>();

    byte[] hashOne = "hashone".getBytes(StandardCharsets.UTF_8), hashTwo = "hashtwo".getBytes(StandardCharsets.UTF_8), hashThree = "hashthree"
        .getBytes(StandardCharsets.UTF_8), hashFour = "hashfour".getBytes(StandardCharsets.UTF_8), hashFive = "hashfive".getBytes(StandardCharsets.UTF_8), hashSix = "hashsix".getBytes(StandardCharsets.UTF_8);

    leaves.add(new MerkleTreeNode(new Range(null, false, "1", false), 0, Collections.<Range> emptyList(), hashOne));
    leaves.add(new MerkleTreeNode(new Range("1", true, "2", false), 0, Collections.<Range> emptyList(), hashTwo));
    leaves.add(new MerkleTreeNode(new Range("2", true, "3", false), 0, Collections.<Range> emptyList(), hashThree));
    leaves.add(new MerkleTreeNode(new Range("3", true, "4", false), 0, Collections.<Range> emptyList(), hashFour));
    leaves.add(new MerkleTreeNode(new Range("4", true, "5", false), 0, Collections.<Range> emptyList(), hashFive));
    leaves.add(new MerkleTreeNode(new Range("5", true, null, false), 0, Collections.<Range> emptyList(), hashSix));

    MerkleTree tree = new MerkleTree(leaves, "MD5");
    MerkleTreeNode root = tree.getRootNode();

    MessageDigest md5 = MessageDigest.getInstance("MD5");
    md5.update(hashOne);
    md5.update(hashTwo);
    byte[] hashOneTwo = md5.digest();

    md5.reset();
    md5.update(hashThree);
    md5.update(hashFour);
    byte[] hashThreeFour = md5.digest();

    md5.reset();
    md5.update(hashFive);
    md5.update(hashSix);
    byte[] hashFiveSix = md5.digest();

    md5.reset();
    md5.update(hashOneTwo);
    md5.update(hashThreeFour);
    byte[] hashOneFour = md5.digest();

    md5.reset();
    md5.update(hashOneFour);
    md5.update(hashFiveSix);
    byte[] rootHash = md5.digest();

    Assert.assertEquals(3, root.getLevel());
    Assert.assertArrayEquals(rootHash, root.getHash());
    Assert.assertEquals(Arrays.asList(new Range(null, false, "4", false), new Range("4", true, null, false)), root.getChildren());
    Assert.assertEquals(new Range(), root.getRange());
  }

}
