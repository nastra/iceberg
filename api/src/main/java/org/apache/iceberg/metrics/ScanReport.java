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
package org.apache.iceberg.metrics;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.MetricsContext.Counter;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** A Table Scan report that contains all relevant information from a Table Scan. */
public class ScanReport implements Serializable {

  private final String tableName;
  private final long snapshotId;
  private final Expression filter;
  private final Schema projection;
  private final int matchingDataFiles;
  private final int matchingDataManifests;
  private final int totalDataManifestsRead;
  private final int addedDataFiles;
  private final int deletedDataFiles;
  private final Duration totalPlanningDuration;
  private final long totalFileSizeBytes;

  private ScanReport(
      String tableName,
      long snapshotId,
      Expression filter,
      Schema projection,
      int matchingDataFiles,
      int matchingDataManifests,
      int totalDataManifestsRead,
      int addedDataFiles,
      int deletedDataFiles,
      Duration totalPlanningDuration,
      long totalFileSizeBytes) {
    this.tableName = tableName;
    this.snapshotId = snapshotId;
    this.filter = filter;
    this.projection = projection;
    this.matchingDataFiles = matchingDataFiles;
    this.matchingDataManifests = matchingDataManifests;
    this.totalDataManifestsRead = totalDataManifestsRead;
    this.addedDataFiles = addedDataFiles;
    this.deletedDataFiles = deletedDataFiles;
    this.totalPlanningDuration = totalPlanningDuration;
    this.totalFileSizeBytes = totalFileSizeBytes;
  }

  public String tableName() {
    return tableName;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public Expression filter() {
    return filter;
  }

  public Schema projection() {
    return projection;
  }

  public int matchingDataFiles() {
    return matchingDataFiles;
  }

  public int matchingDataManifests() {
    return matchingDataManifests;
  }

  public int totalDataManifestsRead() {
    return totalDataManifestsRead;
  }

  public int addedDataFiles() {
    return addedDataFiles;
  }

  public int deletedDataFiles() {
    return deletedDataFiles;
  }

  public Duration totalPlanningDuration() {
    return totalPlanningDuration;
  }

  public long totalFileSizeBytes() {
    return totalFileSizeBytes;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tableName", tableName)
        .add("snapshotId", snapshotId)
        .add("filter", filter)
        .add("projection", projection)
        .add("matchingDataFiles", matchingDataFiles)
        .add("matchingDataManifests", matchingDataManifests)
        .add("totalDataManifestsRead", totalDataManifestsRead)
        .add("addedDataFiles", addedDataFiles)
        .add("deletedDataFiles", deletedDataFiles)
        .add("totalScanDuration", totalPlanningDuration)
        .add("totalFileSizeBytes", totalFileSizeBytes)
        .toString();
  }

  @SuppressWarnings("HiddenField")
  public static class Builder {
    private String tableName;
    private long snapshotId = -1L;
    private Expression filter;
    private Schema projection;
    private ScanMetrics scanMetrics;

    private Builder() {}

    public Builder withTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder withSnapshotId(long snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public Builder withFilter(Expression filter) {
      this.filter = filter;
      return this;
    }

    public Builder withProjection(Schema projection) {
      this.projection = projection;
      return this;
    }

    public Builder fromScanMetrics(ScanMetrics scanMetrics) {
      this.scanMetrics = scanMetrics;
      return this;
    }

    public ScanReport build() {
      Preconditions.checkArgument(null != tableName, "TableName must be non-null");
      Preconditions.checkArgument(null != filter, "Expression filter must be non-null");
      Preconditions.checkArgument(null != projection, "Schema projection must be non-null");
      Preconditions.checkArgument(null != scanMetrics, "ScanMetrics must be non-null");
      return new ScanReport(
          tableName,
          snapshotId,
          filter,
          projection,
          scanMetrics.matchingDataFiles().count().orElse(-1),
          scanMetrics.matchingDataManifests().count().orElse(-1),
          scanMetrics.totalDataManifestsRead().count().orElse(-1),
          scanMetrics.addedDataFiles().count().orElse(-1),
          scanMetrics.deletedDataFiles().count().orElse(-1),
          scanMetrics.totalPlanningDuration().totalDuration(),
          scanMetrics.totalFileSizeInBytes().count().orElse(-1L));
    }
  }

  /** Carries all metrics for a particular scan */
  public static class ScanMetrics {
    public static final ScanMetrics NOOP = new ScanMetrics(MetricsContext.nullMetrics());
    private final Counter<Integer> matchingDataFiles;
    private final Counter<Integer> totalDataManifestsRead;
    private final Counter<Integer> matchingDataManifests;
    private final Counter<Integer> addedDataFiles;
    private final Counter<Integer> deletedDataFiles;
    private final Counter<Long> totalFileSizeInBytes;
    private final Timer totalPlanningDuration;

    public ScanMetrics(MetricsContext metricsContext) {
      Preconditions.checkArgument(null != metricsContext, "MetricsContext must be non-null");
      this.matchingDataFiles =
          metricsContext.counter("matchingDataFiles", Integer.class, MetricsContext.Unit.COUNT);
      this.totalDataManifestsRead =
          metricsContext.counter(
              "totalDataManifestsRead", Integer.class, MetricsContext.Unit.COUNT);
      this.matchingDataManifests =
          metricsContext.counter("matchingDataManifests", Integer.class, MetricsContext.Unit.COUNT);
      this.addedDataFiles =
          metricsContext.counter("addedDataFiles", Integer.class, MetricsContext.Unit.COUNT);
      this.deletedDataFiles =
          metricsContext.counter("deletedDataFiles", Integer.class, MetricsContext.Unit.COUNT);
      this.totalFileSizeInBytes =
          metricsContext.counter("totalFileSizeInBytes", Long.class, MetricsContext.Unit.BYTES);
      this.totalPlanningDuration =
          metricsContext.timer("totalPlanningDuration", TimeUnit.NANOSECONDS);
    }

    public Counter<Integer> matchingDataFiles() {
      return matchingDataFiles;
    }

    public Counter<Integer> totalDataManifestsRead() {
      return totalDataManifestsRead;
    }

    public Counter<Integer> matchingDataManifests() {
      return matchingDataManifests;
    }

    public Counter<Long> totalFileSizeInBytes() {
      return totalFileSizeInBytes;
    }

    public Counter<Integer> addedDataFiles() {
      return addedDataFiles;
    }

    public Counter<Integer> deletedDataFiles() {
      return deletedDataFiles;
    }

    public Timer totalPlanningDuration() {
      return totalPlanningDuration;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("matchingDataFiles", matchingDataFiles)
          .add("totalDataManifestsRead", totalDataManifestsRead)
          .add("matchingDataManifests", matchingDataManifests)
          .add("addedDataFiles", addedDataFiles)
          .add("deletedDataFiles", deletedDataFiles)
          .add("totalFileSizeInBytes", totalFileSizeInBytes)
          .add("totalPlanningDuration", totalPlanningDuration)
          .toString();
    }
  }
}
