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
package org.apache.iceberg.inmemory;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestTransactionalInMemoryCatalog {
  private TransactionalInMemoryCatalog catalog;
  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  @BeforeEach
  public void before() {
    catalog = new TransactionalInMemoryCatalog();
    catalog.initialize("in-memory-catalog", ImmutableMap.of());
  }

  @Test
  public void testSimpleNamespaceAndTableCreation() {
    CatalogTransaction catalogTx = catalog.startTransaction(IsolationLevel.SNAPSHOT);
    Catalog asCatalog = catalogTx.asCatalog();
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);
    assertThat(catalog.namespaceExists(namespace)).isTrue();

    assertThat(asCatalog.tableExists(identifier)).isFalse();
    asCatalog.createTable(identifier, SCHEMA);
    assertThat(asCatalog.tableExists(identifier)).isTrue();
    // should not exist in the non-transactional view of the catalog
    // assertThat(catalog.tableExists(identifier)).isFalse();

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
  public void testThis() {
    CatalogTransaction catalogTx = catalog.startTransaction(IsolationLevel.SERIALIZABLE_WITH_FIXED_READS);
    Catalog asCatalog = catalogTx.asCatalog();
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);
    assertThat(asCatalog.tableExists(identifier)).isFalse();
    catalog.createTable(identifier, SCHEMA);
    assertThat(asCatalog.tableExists(identifier)).isFalse();
    assertThat(catalog.loadTable(identifier)).isNotNull();

    catalogTx.commitTransaction();
  }
}
