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

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Test;

public class TestSnapshotReference {

  @Test
  public void testSnapshotTagReferenceBuilderDefault() {
    SnapshotReference ref = SnapshotReference.builderFor(1, "test", SnapshotReferenceType.TAG).build();
    Assert.assertEquals("test", ref.snapshotName());
    Assert.assertEquals(1, ref.snapshotId());
    Assert.assertEquals(SnapshotReferenceType.TAG, ref.type());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(5), ref.maxSnapshotAgeMs());
  }

  @Test
  public void testSnapshotBranchReferenceBuilderDefault() {
    SnapshotReference ref = SnapshotReference.builderFor(1, "test", SnapshotReferenceType.BRANCH).build();
    Assert.assertEquals("test", ref.snapshotName());
    Assert.assertEquals(1, ref.snapshotId());
    Assert.assertEquals(SnapshotReferenceType.BRANCH, ref.type());
    Assert.assertEquals(1, (int) ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(5), ref.maxSnapshotAgeMs());
  }

  @Test
  public void testSnapshotReferenceBuilderWithOverride() {
    SnapshotReference ref = SnapshotReference.builderFor(1, "test", SnapshotReferenceType.BRANCH)
        .withMinSnapshotsToKeep(10)
        .withMaxSnapshotAgeMs(10)
        .build();
    Assert.assertEquals("test", ref.snapshotName());
    Assert.assertEquals(1, ref.snapshotId());
    Assert.assertEquals(SnapshotReferenceType.BRANCH, ref.type());
    Assert.assertEquals(10, (int) ref.minSnapshotsToKeep());
    Assert.assertEquals(10, ref.maxSnapshotAgeMs());
  }

  @Test
  public void testSnapshotAnnotationBuilderFailures() {
    AssertHelpers.assertThrows("Snapshot ID must be greater than 0",
        ValidationException.class,
        () -> SnapshotReference.builderFor(-2, "test", SnapshotReferenceType.TAG).build());

    AssertHelpers.assertThrows("Max snapshot age must be greater than 0",
        ValidationException.class,
        () -> SnapshotReference.builderFor(1, "test", SnapshotReferenceType.TAG)
            .withMaxSnapshotAgeMs(-1)
            .build());

    AssertHelpers.assertThrows("Snapshot reference name must not be null",
        ValidationException.class,
        () -> SnapshotReference.builderFor(1, null, SnapshotReferenceType.TAG).build());

    AssertHelpers.assertThrows("TAG type snapshot reference does not support setting minSnapshotsToKeep",
        ValidationException.class,
        () -> SnapshotReference.builderFor(1, "test", SnapshotReferenceType.TAG)
            .withMinSnapshotsToKeep(2)
            .build());
  }
}
