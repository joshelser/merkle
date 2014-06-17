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

import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class GenerateHashesTest {

  @Test
  public void emptySplitsToRanges() {
    GenerateHashes generate = new GenerateHashes();
    List<Text> splits = Collections.emptyList();

    // No splits is one range (-inf, +inf)
    TreeSet<Range> ranges = generate.endRowsToRanges(splits);
    Assert.assertEquals(1, ranges.size());

    Range r = Iterables.getOnlyElement(ranges);
    Assert.assertEquals(new Range(), r);
  }

  @Test
  public void singleSplitToRange() {
    GenerateHashes generate = new GenerateHashes();
    List<Text> splits = Collections.singletonList(new Text("x"));

    // One split is two ranges (-inf, x) and [x, +inf)
    TreeSet<Range> ranges = generate.endRowsToRanges(splits);
    Assert.assertEquals(2, ranges.size());

    Range r = ranges.pollFirst(); 
    Assert.assertEquals(new Range(null, false, "x\0", false), r);

    r = ranges.pollFirst();
    Assert.assertEquals(new Range("x\0", true, null, false), r);
  }

}
