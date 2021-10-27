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

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.exceptions.ValidationException;

/**
 * User-defined information of a named snapshot
 */
public class SnapshotReference {

  private final long snapshotId;
  private final String name;
  private final SnapshotReferenceType type;
  private final Integer minSnapshotsToKeep;
  private final long maxSnapshotAgeMs;

  private SnapshotReference(
      long snapshotId,
      String name,
      SnapshotReferenceType type,
      Integer minSnapshotsToKeep,
      long maxSnapshotAgeMs) {
    this.snapshotId = snapshotId;
    this.name = name;
    this.type = type;
    this.minSnapshotsToKeep = minSnapshotsToKeep;
    this.maxSnapshotAgeMs = maxSnapshotAgeMs;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public String snapshotName() {
    return name;
  }

  public SnapshotReferenceType type() {
    return type;
  }

  /**
   * Returns the minimum number of snapshots to keep for a BRANCH, or null for a TAG
   */
  public Integer minSnapshotsToKeep() {
    return minSnapshotsToKeep;
  }

  public long maxSnapshotAgeMs() {
    return maxSnapshotAgeMs;
  }

  public static Builder builderFor(long snapshotId, String name, SnapshotReferenceType type) {
    return new Builder(snapshotId, name, type);
  }

  public static class Builder {

    private final Long snapshotId;
    private final String name;
    private final SnapshotReferenceType type;
    private Integer minSnapshotsToKeep;
    private Long maxSnapshotAgeMs;

    Builder(long snapshotId, String name, SnapshotReferenceType type) {
      ValidationException.check(snapshotId > 0, "Snapshot ID must be greater than 0");
      ValidationException.check(name != null, "Snapshot reference name must not be null");
      this.snapshotId = snapshotId;
      this.name = name;
      this.type = type;
    }

    public Builder withMinSnapshotsToKeep(Integer value) {
      this.minSnapshotsToKeep = value;
      return this;
    }

    public Builder withMaxSnapshotAgeMs(long value) {
      this.maxSnapshotAgeMs = value;
      return this;
    }

    public SnapshotReference build() {
      if (type.equals(SnapshotReferenceType.TAG)) {
        ValidationException.check(minSnapshotsToKeep == null,
            "TAG type snapshot reference does not support setting minSnapshotsToKeep");
      }

      if (type.equals(SnapshotReferenceType.BRANCH) && minSnapshotsToKeep == null) {
        // use the same default as table property history.expire.min-snapshots-to-keep
        minSnapshotsToKeep = 1;
      }

      if (maxSnapshotAgeMs == null) {
        // use the same default as table property history.expire.max-snapshot-age-ms
        maxSnapshotAgeMs = TimeUnit.DAYS.toMillis(5);
      } else {
        ValidationException.check(maxSnapshotAgeMs > 0, "Max snapshot age must be greater than 0");
      }

      return new SnapshotReference(snapshotId, name, type, minSnapshotsToKeep, maxSnapshotAgeMs);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)  {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SnapshotReference that = (SnapshotReference) o;
    return snapshotId == that.snapshotId &&
        Objects.equals(name, that.name) &&
        type == that.type &&
        Objects.equals(minSnapshotsToKeep, that.minSnapshotsToKeep) &&
        maxSnapshotAgeMs == that.maxSnapshotAgeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId, name, type, minSnapshotsToKeep, maxSnapshotAgeMs);
  }

  @Override
  public String toString() {
    return snapshotId + ":" + name + ":" + type +
        (type == SnapshotReferenceType.TAG ? "(maxSnapshotAgeMs=" + maxSnapshotAgeMs + ")" :
        "(minSnapshotsToKeep=" + minSnapshotsToKeep + ",maxSnapshotAgeMs=" + maxSnapshotAgeMs + ")");
  }
}
