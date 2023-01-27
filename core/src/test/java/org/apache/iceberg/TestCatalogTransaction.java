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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

public class TestCatalogTransaction extends TableTestBase {
  private InMemoryCatalog catalog;

  public TestCatalogTransaction() {
    super(2);
  }

  @Before
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(
        "in-memory-catalog",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, metadataDir.getAbsolutePath()));
  }

  @Test
  public void testSingleOperationTransaction() {
    TableIdentifier identifier = TableIdentifier.of("tx-with-single-op");
    catalog.createTable(identifier, SCHEMA, SPEC);

    Table one = catalog.loadTable(identifier);
    TableMetadata base = ((BaseTable) one).operations().current();

    CatalogTransaction catalogTransaction =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    Catalog txCatalog = catalogTransaction.asCatalog();
    txCatalog.loadTable(identifier).newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(base).isSameAs(((BaseTable) one).operations().refresh());
    assertThat(base.currentSnapshot()).isNull();

    catalogTransaction.commitTransaction();

    TableMetadata updated = ((BaseTable) one).operations().refresh();
    assertThat(base).isNotSameAs(updated);
    assertThat(base.lastUpdatedMillis()).isLessThan(updated.lastUpdatedMillis());

    assertThat(updated.currentSnapshot().addedDataFiles(catalog.loadTable(identifier).io()))
        .hasSize(2);
  }

  @Test
  public void testTransactionsAgainstMultipleTables() {
    List<String> tables = Arrays.asList("a", "b", "c");
    for (String tbl : tables) {
      catalog.createTable(TableIdentifier.of(tbl), SCHEMA);
    }

    TableIdentifier a = TableIdentifier.of("a");
    TableIdentifier b = TableIdentifier.of("b");
    TableIdentifier c = TableIdentifier.of("c");
    Table one = catalog.loadTable(a);
    Table two = catalog.loadTable(b);
    Table three = catalog.loadTable(c);

    TableMetadata baseMetadataOne = ((BaseTable) one).operations().current();
    TableMetadata baseMetadataTwo = ((BaseTable) two).operations().current();
    TableMetadata baseMetadataThree = ((BaseTable) three).operations().current();

    CatalogTransaction catalogTransaction =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    Catalog txCatalog = catalogTransaction.asCatalog();

    txCatalog.loadTable(a).newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().current());

    txCatalog.loadTable(b).newDelete().deleteFile(FILE_C).commit();
    txCatalog.loadTable(b).newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().current());

    txCatalog.loadTable(c).newDelete().deleteFile(FILE_A).commit();
    txCatalog.loadTable(c).newAppend().appendFile(FILE_D).commit();

    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().current());
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().current());
    assertThat(baseMetadataThree).isSameAs(((BaseTable) three).operations().current());

    for (String tbl : tables) {
      TableMetadata current =
          ((BaseTable) catalog.loadTable(TableIdentifier.of(tbl))).operations().current();
      assertThat(current.snapshots()).isEmpty();
    }

    catalogTransaction.commitTransaction();

    for (String tbl : tables) {
      TableMetadata current =
          ((BaseTable) catalog.loadTable(TableIdentifier.of(tbl))).operations().current();
      assertThat(current.snapshots()).hasSizeGreaterThanOrEqualTo(1);
    }

    one = catalog.loadTable(a);
    two = catalog.loadTable(b);
    three = catalog.loadTable(c);
    assertThat(one.currentSnapshot().allManifests(one.io())).hasSize(1);
    assertThat(two.currentSnapshot().allManifests(two.io())).hasSize(1);
    assertThat(three.currentSnapshot().allManifests(three.io())).hasSize(1);

    assertThat(one.currentSnapshot().addedDataFiles(one.io())).hasSize(2);
    assertThat(two.currentSnapshot().addedDataFiles(two.io())).hasSize(2);
    assertThat(three.currentSnapshot().addedDataFiles(three.io())).hasSize(1);
  }

  @Test
  public void testTransactionsAgainstMultipleTablesLastOneFails() {
    for (String tbl : Arrays.asList("a", "b", "c")) {
      catalog.createTable(TableIdentifier.of(tbl), SCHEMA);
    }

    TableIdentifier a = TableIdentifier.of("a");
    TableIdentifier b = TableIdentifier.of("b");
    TableIdentifier c = TableIdentifier.of("c");
    Table one = catalog.loadTable(a);
    Table two = catalog.loadTable(b);
    Table three = catalog.loadTable(c);

    TableMetadata baseMetadataOne = ((BaseTable) one).operations().current();
    TableMetadata baseMetadataTwo = ((BaseTable) two).operations().current();
    TableMetadata baseMetadataThree = ((BaseTable) three).operations().current();

    CatalogTransaction catalogTransaction =
        catalog.startTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    Catalog txCatalog = catalogTransaction.asCatalog();
    txCatalog.loadTable(a).newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().current());

    txCatalog.loadTable(b).newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    txCatalog.loadTable(b).newDelete().deleteFile(FILE_C).commit();
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().current());

    txCatalog.loadTable(c).newDelete().deleteFile(FILE_A).commit();
    txCatalog.loadTable(c).newAppend().appendFile(FILE_D).commit();

    txCatalog.loadTable(c).updateSchema().addColumn("new-column", Types.IntegerType.get()).commit();

    assertThat(baseMetadataThree).isSameAs(((BaseTable) three).operations().current());

    // directly update the table for adding "another-column"
    three.updateSchema().addColumn("another-column", Types.IntegerType.get()).commit();

    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().current());
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().current());
    assertThat(baseMetadataThree).isNotSameAs(((BaseTable) three).operations().current());

    Assertions.assertThatThrownBy(catalogTransaction::commitTransaction)
        .isInstanceOf(CommitFailedException.class);

    // the third update in the catalog TX fails, so we need to make sure that all changes from the
    // catalog TX are rolled back
    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().current());
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().current());
    assertThat(baseMetadataThree).isNotSameAs(((BaseTable) three).operations().current());

    assertThat(((BaseTable) one).operations().current().currentSnapshot()).isNull();
    assertThat(((BaseTable) two).operations().current().currentSnapshot()).isNull();
    assertThat(((BaseTable) three).operations().current().currentSnapshot()).isNull();
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
    Catalog txCatalog = catalogTx.asCatalog();

    // this should fail when the catalog TX commits due to SERIALIZABLE not permitting any other
    // updates
    table.updateSchema().addColumn("x", Types.BooleanType.get()).commit();

    txCatalog.loadTable(identifier).updateSchema().addColumn("y", Types.BooleanType.get()).commit();
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
    txCatalog
        .loadTable(identifier)
        .updateSchema()
        .addColumn(column, Types.BooleanType.get())
        .commit();
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
