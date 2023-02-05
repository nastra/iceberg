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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestSnapshotOperations {

  @Test
  public void emptySnapshotsAndRefs() {
    assertThat(SnapshotOperations.empty().snapshots()).isEmpty();
    assertThat(SnapshotOperations.empty().refs()).isEmpty();
  }

  @Test
  public void exactlyOnceLazyLoadingOfSnapshots() {
    Snapshot one = new BaseSnapshot(1L, 1L, 1L, 1L, "op", ImmutableMap.of(), 1, "x");
    Snapshot two = new BaseSnapshot(2L, 2L, 2L, 2L, "op", ImmutableMap.of(), 2, "y");
    Snapshot three = new BaseSnapshot(3L, 3L, 3L, 3L, "op", ImmutableMap.of(), 3, "z");

    List<Snapshot> snapshots = Arrays.asList(one, two, three);
    AtomicInteger counter = new AtomicInteger(0);

    SnapshotOperations snapshotOperations =
        ImmutableSnapshotOperations.builder()
            .initialSnapshots(Collections.singletonList(one))
            .snapshotsSupplier(
                () -> {
                  counter.incrementAndGet();
                  return Arrays.asList(two, three);
                })
            .build();

    // call snapshots() a few times
    for (int i = 0; i < 5; i++) {
      assertThat(snapshotOperations.snapshots()).containsExactlyElementsOf(snapshots);
      assertThat(snapshotOperations.snapshots()).containsExactlyElementsOf(snapshots);
    }

    assertThat(snapshotOperations.snapshot(one.snapshotId())).isEqualTo(one);
    assertThat(snapshotOperations.snapshot(two.snapshotId())).isEqualTo(two);
    assertThat(snapshotOperations.snapshot(three.snapshotId())).isEqualTo(three);

    assertThat(snapshotOperations.contains(one.snapshotId())).isTrue();
    assertThat(snapshotOperations.contains(two.snapshotId())).isTrue();
    assertThat(snapshotOperations.contains(three.snapshotId())).isTrue();

    assertThat(snapshotOperations.contains(-1L)).isFalse();

    // make sure the supplier was called only once
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  public void withAndWithoutSnapshots() {
    Snapshot one = new BaseSnapshot(1L, 1L, 1L, 1L, "op", ImmutableMap.of(), 1, "x");
    Snapshot two = new BaseSnapshot(2L, 2L, 2L, 2L, "op", ImmutableMap.of(), 2, "y");
    Snapshot three = new BaseSnapshot(3L, 3L, 3L, 3L, "op", ImmutableMap.of(), 3, "z");

    List<Snapshot> snapshots = Arrays.asList(one, two, three);
    AtomicInteger counter = new AtomicInteger(0);

    SnapshotOperations snapshotOperations =
        ImmutableSnapshotOperations.builder()
            .initialSnapshots(Collections.singletonList(one))
            .snapshotsSupplier(
                () -> {
                  counter.incrementAndGet();
                  return Collections.singletonList(two);
                })
            .build();

    assertThat(snapshotOperations.snapshots()).containsExactly(one, two);

    SnapshotOperations opsWithSnapshotThree = snapshotOperations.withSnapshot(three);
    assertThat(opsWithSnapshotThree.snapshots()).containsExactlyElementsOf(snapshots);

    assertThat(opsWithSnapshotThree.snapshot(one.snapshotId())).isEqualTo(one);
    assertThat(opsWithSnapshotThree.snapshot(two.snapshotId())).isEqualTo(two);
    assertThat(opsWithSnapshotThree.snapshot(three.snapshotId())).isEqualTo(three);

    SnapshotOperations opsWithoutSnapshotOne = opsWithSnapshotThree.withoutSnapshot(one);
    assertThat(opsWithoutSnapshotOne.snapshots()).containsExactly(two, three);

    assertThat(opsWithoutSnapshotOne.snapshot(one.snapshotId())).isNull();
    assertThat(opsWithoutSnapshotOne.snapshot(two.snapshotId())).isEqualTo(two);
    assertThat(opsWithoutSnapshotOne.snapshot(three.snapshotId())).isEqualTo(three);

    // make sure the supplier was called only once
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  public void exactlyOnceLazyLoadingOfRefs() {
    SnapshotRef one = SnapshotRef.builderFor(1, SnapshotRefType.BRANCH).build();
    SnapshotRef two = SnapshotRef.builderFor(2, SnapshotRefType.BRANCH).build();
    SnapshotRef three = SnapshotRef.builderFor(3, SnapshotRefType.TAG).build();

    Map<String, SnapshotRef> allRefs = ImmutableMap.of("one", one, "two", two, "three", three);

    AtomicInteger counter = new AtomicInteger(0);

    SnapshotOperations snapshotOperations =
        ImmutableSnapshotOperations.builder()
            .initialRefs(ImmutableMap.of("one", one, "two", two))
            .refsSupplier(
                () -> {
                  counter.incrementAndGet();
                  return ImmutableMap.of("three", three);
                })
            .build();

    // call refs() a few times
    for (int i = 0; i < 5; i++) {
      assertThat(snapshotOperations.refs()).containsAllEntriesOf(allRefs);
      assertThat(snapshotOperations.refs()).containsAllEntriesOf(allRefs);
    }

    assertThat(snapshotOperations.ref("one")).isEqualTo(one);
    assertThat(snapshotOperations.ref("two")).isEqualTo(two);
    assertThat(snapshotOperations.ref("three")).isEqualTo(three);

    // make sure the supplier was called only once
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  public void withAndWithoutRefs() {
    SnapshotRef one = SnapshotRef.builderFor(1, SnapshotRefType.BRANCH).build();
    SnapshotRef two = SnapshotRef.builderFor(2, SnapshotRefType.BRANCH).build();
    SnapshotRef three = SnapshotRef.builderFor(3, SnapshotRefType.TAG).build();
    Map<String, SnapshotRef> allRefs = ImmutableMap.of("one", one, "two", two, "three", three);

    AtomicInteger counter = new AtomicInteger(0);

    SnapshotOperations snapshotOperations =
        ImmutableSnapshotOperations.builder()
            .initialRefs(ImmutableMap.of("one", one))
            .refsSupplier(
                () -> {
                  counter.incrementAndGet();
                  return ImmutableMap.of("two", two);
                })
            .build();

    assertThat(snapshotOperations.refs())
        .containsAllEntriesOf(ImmutableMap.of("one", one, "two", two));
    assertThat(snapshotOperations.ref("one")).isEqualTo(one);
    assertThat(snapshotOperations.ref("two")).isEqualTo(two);
    assertThat(snapshotOperations.ref("three")).isNull();

    SnapshotOperations opsWithRefThree = snapshotOperations.withRef("three", three);
    assertThat(opsWithRefThree.refs()).containsAllEntriesOf(allRefs);

    assertThat(opsWithRefThree.ref("one")).isEqualTo(one);
    assertThat(opsWithRefThree.ref("two")).isEqualTo(two);
    assertThat(opsWithRefThree.ref("three")).isEqualTo(three);

    SnapshotOperations opsWithoutRefOne = opsWithRefThree.withoutRef("one");
    assertThat(opsWithoutRefOne.refs())
        .containsAllEntriesOf(ImmutableMap.of("two", two, "three", three));
    assertThat(opsWithoutRefOne.ref("one")).isNull();
    assertThat(opsWithoutRefOne.ref("two")).isEqualTo(two);
    assertThat(opsWithoutRefOne.ref("three")).isEqualTo(three);

    // make sure the supplier was called only once
    assertThat(counter.get()).isEqualTo(1);
  }
}
