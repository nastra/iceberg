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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableSupplier;
import org.immutables.value.Value;

@Value.Immutable
public abstract class SnapshotOperations implements Serializable {

  @Value.Default
  List<Snapshot> initialSnapshots() {
    return ImmutableList.of();
  }

  @Value.Default
  SerializableSupplier<List<Snapshot>> snapshotsSupplier() {
    return ImmutableList::of;
  }

  @Value.Default
  Map<String, SnapshotRef> initialRefs() {
    return ImmutableMap.of();
  }

  @Value.Default
  SerializableSupplier<Map<String, SnapshotRef>> refsSupplier() {
    return ImmutableMap::of;
  }

  @Value.Lazy
  List<Snapshot> snapshots() {
    return ImmutableList.<Snapshot>builder()
        .addAll(initialSnapshots())
        .addAll(snapshotsSupplier().get())
        .build();
  }

  @Value.Lazy
  Map<Long, Snapshot> snapshotsById() {
    return snapshots().stream()
        .collect(Collectors.toMap(Snapshot::snapshotId, Function.identity()));
  }

  @Value.Lazy
  public Map<String, SnapshotRef> refs() {
    return ImmutableMap.<String, SnapshotRef>builder()
        .putAll(initialRefs())
        .putAll(refsSupplier().get())
        .build();
  }

  Snapshot snapshot(long id) {
    return snapshotsById().get(id);
  }

  boolean contains(long id) {
    return snapshotsById().containsKey(id);
  }

  SnapshotRef ref(String name) {
    return refs().get(name);
  }

  static SnapshotOperations empty() {
    return ImmutableSnapshotOperations.builder().build();
  }

  public SnapshotOperations withoutRef(String ref) {
    Map<String, SnapshotRef> refs = Maps.newHashMap(refs());
    refs.remove(ref);
    return ImmutableSnapshotOperations.builder()
        .from(this)
        .initialRefs(refs)
        .refsSupplier(ImmutableMap::of)
        .build();
  }

  public SnapshotOperations withRef(String ref, SnapshotRef snapshotRef) {
    Map<String, SnapshotRef> refs = Maps.newHashMap(refs());
    refs.put(ref, snapshotRef);
    return ImmutableSnapshotOperations.builder()
        .from(this)
        .initialRefs(refs)
        .refsSupplier(ImmutableMap::of)
        .build();
  }

  public SnapshotOperations withSnapshot(Snapshot snapshot) {
    List<Snapshot> snapshots = Lists.newArrayList(snapshots());
    snapshots.add(snapshot);
    return ImmutableSnapshotOperations.builder()
        .from(this)
        .initialSnapshots(snapshots)
        .snapshotsSupplier(ImmutableList::of)
        .build();
  }

  public SnapshotOperations withoutSnapshot(Snapshot snapshot) {
    List<Snapshot> snapshots = Lists.newArrayList(snapshots());
    snapshots.remove(snapshot);
    return ImmutableSnapshotOperations.builder()
        .from(this)
        .initialSnapshots(snapshots)
        .snapshotsSupplier(ImmutableList::of)
        .build();
  }

  void validate(long currentSnapshotId, long lastSequenceNumber) {
    validateSnapshots(lastSequenceNumber);
    validateRefs(currentSnapshotId);
  }

  private void validateSnapshots(long lastSequenceNumber) {
    for (Snapshot snap : snapshotsSupplier().get()) {
      ValidationException.check(
          snap.sequenceNumber() <= lastSequenceNumber,
          "Invalid snapshot with sequence number %s greater than last sequence number %s",
          snap.sequenceNumber(),
          lastSequenceNumber);
    }
  }

  private void validateRefs(long currentSnapshotId) {
    for (SnapshotRef ref : refs().values()) {
      Preconditions.checkArgument(
          snapshotsById().containsKey(ref.snapshotId()),
          "Snapshot for reference %s does not exist in the existing snapshots list",
          ref);
    }

    SnapshotRef main = refs().get(SnapshotRef.MAIN_BRANCH);
    if (currentSnapshotId != -1) {
      Preconditions.checkArgument(
          main == null || currentSnapshotId == main.snapshotId(),
          "Current snapshot ID does not match main branch (%s != %s)",
          currentSnapshotId,
          main != null ? main.snapshotId() : null);
    } else {
      Preconditions.checkArgument(
          main == null, "Current snapshot is not set, but main branch exists: %s", main);
    }
  }
}
