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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.TransactionalInMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

public class TestCatalogTransaction extends TableTestBase {
  private TransactionalInMemoryCatalog catalog;

  public TestCatalogTransaction() {
    super(2);
  }

  @Before
  public void before() {
    catalog = new TransactionalInMemoryCatalog();
    catalog.initialize("in-memory-catalog", ImmutableMap.of());
  }

  @Test
  public void testSingleOperationTransaction() {
    Table one =
        TestTables.create(
            tableDir, "tx-with-single-op", SCHEMA, SPEC, SortOrder.unsorted(), formatVersion);

    assertThat(TestTables.metadataVersion(one.name())).isEqualTo(0L);

    TableMetadata base = TestTables.readMetadata(one.name());

    CatalogTransaction catalogTransaction =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
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

    CatalogTransaction catalogTransaction =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
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

    CatalogTransaction catalogTransaction =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
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

    // directly update the table for adding "another-column"
    three.updateSchema().addColumn("another-column", Types.IntegerType.get()).commit();

    assertThat(baseMetadataOne).isSameAs(TestTables.readMetadata(one.name()));
    assertThat(baseMetadataTwo).isSameAs(TestTables.readMetadata(two.name()));
    assertThat(baseMetadataThree).isNotSameAs(TestTables.readMetadata(three.name()));

    Assertions.assertThatThrownBy(catalogTransaction::commitTransaction)
        .isInstanceOf(CommitFailedException.class);

    // the third update in the catalog TX fails, so we need to make sure that all changes from the
    // catalog TX are rolled back
    assertThat(baseMetadataOne).isSameAs(TestTables.readMetadata(one.name()));
    assertThat(baseMetadataTwo).isSameAs(TestTables.readMetadata(two.name()));
    assertThat(baseMetadataThree).isNotSameAs(TestTables.readMetadata(three.name()));

    assertThat(TestTables.readMetadata(one.name()).currentSnapshot()).isNull();
    assertThat(TestTables.readMetadata(two.name()).currentSnapshot()).isNull();
    assertThat(TestTables.readMetadata(three.name()).currentSnapshot()).isNull();
  }

  @Test
  public void testSimpleNamespaceAndTableCreation() {
    CatalogTransaction catalogTx =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    Catalog asCatalog = catalogTx.asCatalog();
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);
    assertThat(catalog.namespaceExists(namespace)).isTrue();

    assertThat(asCatalog.tableExists(identifier)).isFalse();
    asCatalog.createTable(identifier, SCHEMA);
    assertThat(asCatalog.tableExists(identifier)).isTrue();

    TableIdentifier to = TableIdentifier.of(namespace, "table2");
    asCatalog.renameTable(identifier, to);
    assertThat(asCatalog.tableExists(identifier)).isFalse();
    assertThat(asCatalog.tableExists(to)).isTrue();
    assertThat(catalog.tableExists(identifier)).isFalse();

    catalogTx.commitTransaction();
    assertThat(catalog.namespaceExists(namespace)).isTrue();
    assertThat(catalog.tableExists(identifier)).isFalse();
    assertThat(catalog.tableExists(to)).isTrue();
  }

  @Test
  public void testSerializableFailsOnExternalChanges() {
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);
    Table table = catalog.createTable(identifier, SCHEMA);
    assertThat(catalog.tableExists(identifier)).isTrue();

    CatalogTransaction catalogTx =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SERIALIZABLE_WITH_FIXED_READS);

    // this should fail when the catalog TX commits due to SERIALIZABLE not permitting any other
    // updates
    table.updateSchema().addColumn("x", Types.BooleanType.get()).commit();

    catalogTx.updateSchema(table).addColumn("y", Types.BooleanType.get()).commit();
    assertThatThrownBy(catalogTx::commitTransaction)
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Found updates at")
        .hasMessageContaining(
            "at isolation level SERIALIZABLE_WITH_FIXED_READS after TX start time");
  }

  @Test
  public void testSchemaUpdateVisibility() {
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);
    Table table = catalog.createTable(identifier, SCHEMA);
    assertThat(catalog.tableExists(identifier)).isTrue();

    CatalogTransaction catalogTx =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SERIALIZABLE_WITH_FIXED_READS);

    Catalog txCatalog = catalogTx.asCatalog();

    String column = "new_col";

    assertThat(txCatalog.loadTable(identifier).schema().findField(column)).isNull();
    catalogTx.updateSchema(table).addColumn(column, Types.BooleanType.get()).commit();
    // changes inside the catalog TX should be visible
    assertThat(txCatalog.loadTable(identifier).schema().findField(column)).isNotNull();

    // changes outside the catalog TX should not be visible
    assertThat(catalog.loadTable(identifier).schema().findField(column)).isNull();

    catalogTx.commitTransaction();

    assertThat(catalog.loadTable(identifier).schema().findField(column)).isNotNull();
    assertThat(txCatalog.loadTable(identifier).schema().findField(column)).isNotNull();
  }

  @Test
  public void testSerializableLoadTableAtTxStartTime() {
    CatalogTransaction.IsolationLevel isolationLevel =
        CatalogTransaction.IsolationLevel.SERIALIZABLE_WITH_FIXED_READS;
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);
    catalog.createTable(identifier, SCHEMA);
    assertThat(catalog.tableExists(identifier)).isTrue();
    catalog.loadTable(identifier).newFastAppend().appendFile(FILE_A).commit();

    Snapshot initialSnapshot = catalog.loadTable(identifier).currentSnapshot();

    CatalogTransaction catalogTx = catalog.startTransaction(isolationLevel);
    Catalog txCatalog = catalogTx.asCatalog();

    catalog.loadTable(identifier).newFastAppend().appendFile(FILE_B).commit();
    catalog.loadTable(identifier).newFastAppend().appendFile(FILE_C).commit();

    Snapshot currentSnapshot = catalog.loadTable(identifier).currentSnapshot();
    Snapshot txSnapshot = txCatalog.loadTable(identifier).currentSnapshot();

    // with SERIALIZABLE_WITH_FIXED_READS the table needs to point to the initial snapshot at TX
    // start time
    assertThat(txSnapshot).isEqualTo(initialSnapshot);
    assertThat(txSnapshot.timestampMillis()).isLessThan(currentSnapshot.timestampMillis());
  }

  @Test
  public void testSerializableLoadTable() {
    CatalogTransaction.IsolationLevel isolationLevel =
        CatalogTransaction.IsolationLevel.SERIALIZABLE_WITH_LOADING_READS;
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);
    catalog.createTable(identifier, SCHEMA);
    assertThat(catalog.tableExists(identifier)).isTrue();
    catalog.loadTable(identifier).newFastAppend().appendFile(FILE_A).commit();

    Snapshot initialSnapshot = catalog.loadTable(identifier).currentSnapshot();

    CatalogTransaction catalogTx = catalog.startTransaction(isolationLevel);
    Catalog txCatalog = catalogTx.asCatalog();

    catalog.loadTable(identifier).newFastAppend().appendFile(FILE_B).commit();
    catalog.loadTable(identifier).newFastAppend().appendFile(FILE_C).commit();

    Snapshot currentSnapshot = catalog.loadTable(identifier).currentSnapshot();
    Snapshot txSnapshot = txCatalog.loadTable(identifier).currentSnapshot();

    // with SERIALIZABLE_WITH_FIXED_READS the table needs to point to the initial snapshot at TX
    // start time
    assertThat(txSnapshot).isEqualTo(currentSnapshot);
    assertThat(txSnapshot.timestampMillis()).isGreaterThan(initialSnapshot.timestampMillis());
  }

  @Test
  public void testSerializableLoadTableWithScan() {
    CatalogTransaction.IsolationLevel isolationLevel =
        CatalogTransaction.IsolationLevel.SERIALIZABLE_WITH_FIXED_READS;
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);
    catalog.createTable(identifier, SCHEMA, SPEC);
    assertThat(catalog.tableExists(identifier)).isTrue();
    catalog.loadTable(identifier).newAppend().appendFile(FILE_A).appendFile(FILE_D).commit();

    CatalogTransaction catalogTx = catalog.startTransaction(isolationLevel);
    Catalog txCatalog = catalogTx.asCatalog();

    catalog.loadTable(identifier).newAppend().appendFile(FILE_B).appendFile(FILE_C).commit();

    assertThat(Iterables.size(catalog.loadTable(identifier).newScan().planFiles())).isEqualTo(4);
    assertThat(Iterables.size(txCatalog.loadTable(identifier).newScan().planFiles())).isEqualTo(2);
  }
}
