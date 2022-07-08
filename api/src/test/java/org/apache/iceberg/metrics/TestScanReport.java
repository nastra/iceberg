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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestScanReport {

  @Test
  public void missingFields() {
    Assertions.assertThatThrownBy(() -> ScanReport.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("TableName must be non-null");

    Assertions.assertThatThrownBy(() -> ScanReport.builder().withTableName("x").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Expression filter must be non-null");

    Assertions.assertThatThrownBy(
            () ->
                ScanReport.builder()
                    .withTableName("x")
                    .withFilter(Expressions.alwaysTrue())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Schema projection must be non-null");

    Assertions.assertThatThrownBy(
            () ->
                ScanReport.builder()
                    .withTableName("x")
                    .withFilter(Expressions.alwaysTrue())
                    .withProjection(
                        new Schema(
                            Types.NestedField.required(1, "c1", Types.StringType.get(), "c1")))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ScanMetrics must be non-null");
  }

  @Test
  public void fromEmptyScanMetrics() {
    String tableName = "x";
    True filter = Expressions.alwaysTrue();
    Schema projection =
        new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    ScanReport scanReport =
        ScanReport.builder()
            .withTableName(tableName)
            .withFilter(filter)
            .withProjection(projection)
            .fromScanMetrics(ScanReport.ScanMetrics.NOOP)
            .build();

    Assertions.assertThat(scanReport.tableName()).isEqualTo(tableName);
    Assertions.assertThat(scanReport.projection()).isEqualTo(projection);
    Assertions.assertThat(scanReport.filter()).isEqualTo(filter);
    Assertions.assertThat(scanReport.snapshotId()).isEqualTo(-1);
    Assertions.assertThat(scanReport.addedDataFiles()).isEqualTo(-1);
    Assertions.assertThat(scanReport.totalPlanningDuration()).isEqualTo(Duration.ZERO);
    Assertions.assertThat(scanReport.deletedDataFiles()).isEqualTo(-1);
    Assertions.assertThat(scanReport.matchingDataFiles()).isEqualTo(-1);
    Assertions.assertThat(scanReport.matchingDataManifests()).isEqualTo(-1);
    Assertions.assertThat(scanReport.totalDataManifestsRead()).isEqualTo(-1);
    Assertions.assertThat(scanReport.totalFileSizeBytes()).isEqualTo(-1L);
  }

  @Test
  public void fromScanMetrics() {
    ScanReport.ScanMetrics scanMetrics = new ScanReport.ScanMetrics(new DefaultMetricsContext());
    scanMetrics.addedDataFiles().increment(5);
    scanMetrics.deletedDataFiles().increment(5);
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.matchingDataFiles().increment(5);
    scanMetrics.totalDataManifestsRead().increment(5);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.matchingDataManifests().increment(5);

    String tableName = "x";
    True filter = Expressions.alwaysTrue();
    Schema projection =
        new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    ScanReport scanReport =
        ScanReport.builder()
            .withTableName(tableName)
            .withFilter(filter)
            .withProjection(projection)
            .withSnapshotId(23L)
            .fromScanMetrics(scanMetrics)
            .build();

    Assertions.assertThat(scanReport.tableName()).isEqualTo(tableName);
    Assertions.assertThat(scanReport.projection()).isEqualTo(projection);
    Assertions.assertThat(scanReport.filter()).isEqualTo(filter);
    Assertions.assertThat(scanReport.snapshotId()).isEqualTo(23L);
    Assertions.assertThat(scanReport.addedDataFiles()).isEqualTo(5);
    Assertions.assertThat(scanReport.totalPlanningDuration()).isEqualTo(Duration.ofMinutes(10L));
    Assertions.assertThat(scanReport.deletedDataFiles()).isEqualTo(5);
    Assertions.assertThat(scanReport.matchingDataFiles()).isEqualTo(5);
    Assertions.assertThat(scanReport.totalDataManifestsRead()).isEqualTo(5);
    Assertions.assertThat(scanReport.matchingDataManifests()).isEqualTo(5);
    Assertions.assertThat(scanReport.totalFileSizeBytes()).isEqualTo(1024L);
  }
}
