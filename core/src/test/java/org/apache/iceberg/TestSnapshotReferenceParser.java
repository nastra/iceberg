/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import org.junit.Assert;
import org.junit.Test;

public class TestSnapshotReferenceParser {

  @Test
  public void testSnapshotTagReferenceToJson() {
    String result = "{\"snapshot-id\":1,\"name\":\"test\",\"type\":\"TAG\",\"max-snapshot-age-ms\":1}";
    SnapshotReference ref = SnapshotReference.builderFor(1, "test", SnapshotReferenceType.TAG)
        .withMaxSnapshotAgeMs(1)
        .build();
    Assert.assertEquals("Can serialize snapshot tag reference",
        result, SnapshotReferenceParser.toJson(ref));
  }

  @Test
  public void testSnapshotBranchReferenceToJson() {
    String json = "{\"snapshot-id\":1,\"name\":\"test\",\"type\":\"BRANCH\"," +
        "\"min-snapshots-to-keep\":2,\"max-snapshot-age-ms\":1}";
    SnapshotReference ref = SnapshotReference.builderFor(1, "test", SnapshotReferenceType.BRANCH)
        .withMinSnapshotsToKeep(2)
        .withMaxSnapshotAgeMs(1)
        .build();
    Assert.assertEquals("Can deserialize snapshot branch reference",
        ref, SnapshotReferenceParser.fromJson(json));
  }

  @Test
  public void testSnapshotTagReferenceFromJson() {
    String json = "{\"snapshot-id\":1,\"name\":\"test\",\"type\":\"TAG\",\"max-snapshot-age-ms\":1}";
    SnapshotReference ref = SnapshotReference.builderFor(1, "test", SnapshotReferenceType.TAG)
        .withMaxSnapshotAgeMs(1)
        .build();
    Assert.assertEquals("Can deserialize snapshot tag reference",
        ref, SnapshotReferenceParser.fromJson(json));
  }

  @Test
  public void testSnapshotBranchReferenceFromJson() {
    String result = "{\"snapshot-id\":1,\"name\":\"test\",\"type\":\"BRANCH\"," +
        "\"min-snapshots-to-keep\":2,\"max-snapshot-age-ms\":1}";
    SnapshotReference ref = SnapshotReference.builderFor(1, "test", SnapshotReferenceType.BRANCH)
        .withMinSnapshotsToKeep(2)
        .withMaxSnapshotAgeMs(1)
        .build();
    Assert.assertEquals("Can serialize snapshot branch reference",
        result, SnapshotReferenceParser.toJson(ref));
  }
}
