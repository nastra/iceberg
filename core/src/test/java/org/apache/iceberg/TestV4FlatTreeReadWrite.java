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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end round trip for the v4 flat-tree prototype: a fast append writes a Parquet root
 * manifest with inlined DATA entries, and a scan plans directly from it via {@link
 * ScanTaskPlanner}.
 */
public class TestV4FlatTreeReadWrite {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  private static final DataFile FILE_A =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("file:/tmp/v4/data-a.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(3)
          .withFormat(FileFormat.PARQUET)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("file:/tmp/v4/data-b.parquet")
          .withFileSizeInBytes(120)
          .withRecordCount(1)
          .withFormat(FileFormat.PARQUET)
          .build();

  @TempDir private File tableDir;

  @Test
  public void testFastAppendWritesRootManifest() throws IOException {
    Table table = TestTables.create(tableDir, "v4_root", SCHEMA, PartitionSpec.unpartitioned(), 4);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.manifestListLocation())
        .as("v4 snapshot points at a Parquet root manifest")
        .endsWith(".parquet")
        .doesNotContain("snap-");
  }

  @Test
  public void testScanPlansInlinedDataEntries() throws IOException {
    Table table = TestTables.create(tableDir, "v4_scan", SCHEMA, PartitionSpec.unpartitioned(), 4);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(scanLocations(table))
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
  }

  @Test
  public void testSecondAppendCarriesForwardParentEntries() throws IOException {
    Table table = TestTables.create(tableDir, "v4_carry", SCHEMA, PartitionSpec.unpartitioned(), 4);

    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    assertThat(scanLocations(table))
        .as("second append carries forward the first append's data entries")
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
  }

  private static List<String> scanLocations(Table table) throws IOException {
    List<String> locations = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        locations.add(task.file().location());
      }
    }

    return locations;
  }
}
