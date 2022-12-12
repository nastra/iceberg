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
import java.util.List;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestCatalogTransaction extends TableTestBase {

  public TestCatalogTransaction() {
    super(2);
  }

  @Test
  public void testSingleOperationTransaction() {
    Table one =
        TestTables.create(
            tableDir, "tx-with-single-op", SCHEMA, SPEC, SortOrder.unsorted(), formatVersion);

    assertThat(TestTables.metadataVersion(one.name())).isEqualTo(0L);

    TableMetadata base = TestTables.readMetadata(one.name());

    BaseCatalogTransaction catalogTransaction = new BaseCatalogTransaction(null, null);
    catalogTransaction.newAppend(one).appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(TestTables.metadataVersion(one.name())).isEqualTo(0L);
    assertThat(base).isSameAs(TestTables.readMetadata(one.name()));

    catalogTransaction.commitTransaction();

    assertThat(TestTables.metadataVersion(one.name())).isEqualTo(1L);
    validateSnapshot(
        base.currentSnapshot(),
        TestTables.readMetadata(one.name()).currentSnapshot(),
        FILE_A,
        FILE_B);
  }

  @Test
  public void testTransactionsAgainstMultipleTables() {
    Table one =
        TestTables.create(tableDir, "one", SCHEMA, SPEC, SortOrder.unsorted(), formatVersion);
    Table two =
        TestTables.create(tableDir, "two", SCHEMA, SPEC, SortOrder.unsorted(), formatVersion);
    Table three =
        TestTables.create(tableDir, "three", SCHEMA, SPEC, SortOrder.unsorted(), formatVersion);

    List<Table> tables = Arrays.asList(one, two, three);
    for (Table tbl : tables) {
      assertThat(TestTables.metadataVersion(tbl.name())).isEqualTo(0L);
    }

    TableMetadata baseMetadataOne = TestTables.readMetadata(one.name());
    TableMetadata baseMetadataTwo = TestTables.readMetadata(two.name());
    TableMetadata baseMetadataThree = TestTables.readMetadata(three.name());

    BaseCatalogTransaction catalogTransaction = new BaseCatalogTransaction(null, null);
    catalogTransaction.newAppend(one).appendFile(FILE_A).appendFile(FILE_B).commit();
    assertThat(baseMetadataOne).isSameAs(TestTables.readMetadata(one.name()));

    catalogTransaction.newFastAppend(two).appendFile(FILE_B).appendFile(FILE_C).commit();
    catalogTransaction.newDelete(two).deleteFile(FILE_C).commit();
    assertThat(baseMetadataTwo).isSameAs(TestTables.readMetadata(two.name()));

    catalogTransaction.newDelete(three).deleteFile(FILE_A).commit();
    catalogTransaction.newAppend(three).appendFile(FILE_D).commit();

    assertThat(baseMetadataOne).isSameAs(TestTables.readMetadata(one.name()));
    assertThat(baseMetadataTwo).isSameAs(TestTables.readMetadata(two.name()));
    assertThat(baseMetadataThree).isSameAs(TestTables.readMetadata(three.name()));

    for (Table tbl : tables) {
      assertThat(TestTables.metadataVersion(tbl.name())).isEqualTo(0L);
    }

    catalogTransaction.commitTransaction();

    for (Table tbl : tables) {
      assertThat(TestTables.metadataVersion(tbl.name())).isEqualTo(1L);
    }

    assertThat(one.currentSnapshot().snapshotId()).isEqualTo(1L);
    assertThat(two.currentSnapshot().snapshotId()).isEqualTo(2L);
    assertThat(three.currentSnapshot().snapshotId()).isEqualTo(2L);

    assertThat(TestTables.readMetadata(one.name()).currentSnapshot().allManifests(table.io()))
        .hasSize(1);
    assertThat(TestTables.readMetadata(two.name()).currentSnapshot().allManifests(table.io()))
        .hasSize(1);
    assertThat(TestTables.readMetadata(three.name()).currentSnapshot().allManifests(table.io()))
        .hasSize(1);

    validateManifestEntries(
        TestTables.readMetadata(one.name()).currentSnapshot().allManifests(table.io()).get(0),
        ids(1L, 1L),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    validateManifestEntries(
        TestTables.readMetadata(two.name()).currentSnapshot().allManifests(table.io()).get(0),
        ids(1L, 2L),
        files(FILE_B, FILE_C),
        statuses(Status.EXISTING, Status.DELETED));

    validateManifestEntries(
        TestTables.readMetadata(three.name()).currentSnapshot().allManifests(table.io()).get(0),
        ids(2L),
        files(FILE_D),
        statuses(Status.ADDED));
  }

  @Test
  public void testTransactionsAgainstMultipleTablesLastOneFails() {
    Table one = TestTables.create(tableDir, "a", SCHEMA, SPEC, SortOrder.unsorted(), formatVersion);
    Table two = TestTables.create(tableDir, "b", SCHEMA, SPEC, SortOrder.unsorted(), formatVersion);
    Table three =
        TestTables.create(tableDir, "c", SCHEMA, SPEC, SortOrder.unsorted(), formatVersion);

    List<Table> tables = Arrays.asList(one, two, three);
    for (Table tbl : tables) {
      assertThat(TestTables.metadataVersion(tbl.name())).isEqualTo(0L);
    }

    TableMetadata baseMetadataOne = TestTables.readMetadata(one.name());
    TableMetadata baseMetadataTwo = TestTables.readMetadata(two.name());
    TableMetadata baseMetadataThree = TestTables.readMetadata(three.name());

    BaseCatalogTransaction catalogTransaction = new BaseCatalogTransaction(null, null);
    // Table table1 = catalogTransaction.asCatalog().loadTable(TableIdentifier.parse(one.name()));
    catalogTransaction.newAppend(one).appendFile(FILE_A).appendFile(FILE_B).commit();
    assertThat(baseMetadataOne).isSameAs(TestTables.readMetadata(one.name()));

    catalogTransaction.newFastAppend(two).appendFile(FILE_B).appendFile(FILE_C).commit();
    catalogTransaction.newDelete(two).deleteFile(FILE_C).commit();
    assertThat(baseMetadataTwo).isSameAs(TestTables.readMetadata(two.name()));

    catalogTransaction.newDelete(three).deleteFile(FILE_A).commit();
    catalogTransaction.newAppend(three).appendFile(FILE_D).commit();

    catalogTransaction
        .updateSchema(three)
        .addColumn("new-column", Types.IntegerType.get())
        .commit();

    assertThat(baseMetadataThree).isSameAs(TestTables.readMetadata(three.name()));
    int schemaId = three.schema().schemaId();

    // directly update the table for adding "another-column" (which causes in-progress txn commit
    // fail)
    three.updateSchema().addColumn("another-column", Types.IntegerType.get()).commit();
    int conflictingSchemaId = three.schema().schemaId();

    assertThat(baseMetadataOne).isSameAs(TestTables.readMetadata(one.name()));
    assertThat(baseMetadataTwo).isSameAs(TestTables.readMetadata(two.name()));
    assertThat(baseMetadataThree).isNotSameAs(TestTables.readMetadata(three.name()));

    Assertions.assertThatThrownBy(catalogTransaction::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Table metadata refresh is required");

    assertThat(baseMetadataOne).isSameAs(TestTables.readMetadata(one.name()));
    assertThat(baseMetadataTwo).isSameAs(TestTables.readMetadata(two.name()));
    assertThat(baseMetadataThree).isNotSameAs(TestTables.readMetadata(three.name()));

    assertThat(TestTables.readMetadata(one.name()).currentSnapshot()).isNull();
    assertThat(TestTables.readMetadata(two.name()).currentSnapshot()).isNull();
    assertThat(TestTables.readMetadata(three.name()).currentSnapshot()).isNull();
  }
}
