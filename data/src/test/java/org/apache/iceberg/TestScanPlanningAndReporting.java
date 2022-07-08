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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.metrics.LoggingScanReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReporter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

public class TestScanPlanningAndReporting extends TableTestBase {

  private final Schema schema =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "x", Types.StringType.get()));

  PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).build();

  private Table table;
  private TestScanReporter reporter;

  public TestScanPlanningAndReporting() {
    super(2);
  }

  @Before
  @Override
  public void setupTable() throws Exception {
    super.setupTable();
    reporter = new TestScanReporter();
    table =
        TestTables.create(
            tableDir,
            "scan-planning-x",
            schema,
            partitionSpec,
            SortOrder.unsorted(),
            formatVersion,
            reporter);
    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", 1);
    record.setField("x", "23");
    GenericRecord record2 = GenericRecord.create(schema);
    record2.setField("id", 2);
    record2.setField("x", "30");
    GenericRecord record3 = GenericRecord.create(schema);
    record3.setField("id", 3);
    record3.setField("x", "45");
    GenericRecord record4 = GenericRecord.create(schema);
    record4.setField("id", 3);
    record4.setField("x", "51");
    DataFile dataFile = writeParquetFile(table, Arrays.asList(record, record3));
    DataFile dataFile2 = writeParquetFile(table, Arrays.asList(record2));
    DataFile dataFile3 = writeParquetFile(table, Arrays.asList(record4));
    table.newFastAppend().appendFile(dataFile).appendFile(dataFile2).appendFile(dataFile3).commit();
    table.refresh();
  }

  @Test
  public void testScanPlanningWithReport() throws IOException {
    TableScan tableScan = table.newScan();

    // should be 3 files
    try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo("scan-planning-x");
    assertThat(scanReport.snapshotId()).isEqualTo(1L);
    assertThat(scanReport.filter()).isEqualTo(Expressions.alwaysTrue());
    assertThat(scanReport.totalPlanningDuration()).isGreaterThan(Duration.ZERO);
    assertThat(scanReport.matchingDataFiles()).isEqualTo(3);
    assertThat(scanReport.matchingDataManifests()).isEqualTo(1);

    // should be two files
    try (CloseableIterable<FileScanTask> fileScanTasks =
        tableScan.filter(Expressions.greaterThan("x", "30")).planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo("scan-planning-x");
    assertThat(scanReport.snapshotId()).isEqualTo(1L);
    assertThat(scanReport.totalPlanningDuration()).isGreaterThan(Duration.ZERO);
    assertThat(scanReport.matchingDataFiles()).isEqualTo(2);
    assertThat(scanReport.matchingDataManifests()).isEqualTo(1);

    // should be 1 file
    try (CloseableIterable<FileScanTask> fileScanTasks =
        tableScan.filter(Expressions.lessThan("x", "30")).planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo("scan-planning-x");
    assertThat(scanReport.snapshotId()).isEqualTo(1L);
    assertThat(scanReport.totalPlanningDuration()).isGreaterThan(Duration.ZERO);
    assertThat(scanReport.matchingDataFiles()).isEqualTo(1);
    assertThat(scanReport.matchingDataManifests()).isEqualTo(1);

    // all files
    try (CloseableIterable<FileScanTask> fileScanTasks =
        tableScan.filter(Expressions.lessThan("x", "52")).planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo("scan-planning-x");
    assertThat(scanReport.snapshotId()).isEqualTo(1L);
    assertThat(scanReport.totalPlanningDuration()).isGreaterThan(Duration.ZERO);
    assertThat(scanReport.matchingDataFiles()).isEqualTo(3);
    assertThat(scanReport.matchingDataManifests()).isEqualTo(1);
  }

  private DataFile writeParquetFile(Table tbl, List<GenericRecord> records) throws IOException {
    File parquetFile = temp.newFile();
    assertTrue(parquetFile.delete());
    FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(tbl.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build();
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    PartitionKey partitionKey = new PartitionKey(tbl.spec(), tbl.schema());
    partitionKey.partition(records.get(0));

    return DataFiles.builder(tbl.spec())
        .withPartition(partitionKey)
        .withInputFile(localInput(parquetFile))
        .withMetrics(appender.metrics())
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  private static class TestScanReporter implements ScanReporter {
    private final List<ScanReport> reports = Lists.newArrayList();
    // this is mainly so that we see scan reports being logged during tests
    private final LoggingScanReporter delegate = new LoggingScanReporter();

    @Override
    public void reportScan(ScanReport scanReport) {
      reports.add(scanReport);
      delegate.reportScan(scanReport);
    }

    @Override
    public ScanReport.ScanMetrics newScanMetrics() {
      return delegate.newScanMetrics();
    }

    public ScanReport lastReport() {
      if (reports.isEmpty()) {
        return null;
      }
      return reports.get(reports.size() - 1);
    }
  }
}
