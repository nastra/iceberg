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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.BaseTransaction.TransactionTable;
import org.apache.iceberg.BaseTransaction.TransactionType;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;

public class BaseCatalogTransaction implements CatalogTransaction {
  private final Map<TableIdentifier, Transaction> txByTable;
  private final Map<TableIdentifier, List<PendingUpdate>> updatesByTable;
  private final IsolationLevel isolationLevel;
  private final TransactionalCatalog origin;
  // private final List<Requirements> requirements;
  private boolean hasCommitted = false;
  private final Instant startTime;

  public BaseCatalogTransaction(TransactionalCatalog origin, IsolationLevel isolationLevel) {
    this.origin = origin;
    this.isolationLevel = isolationLevel;
    this.txByTable = Maps.newConcurrentMap();
    this.updatesByTable = Maps.newConcurrentMap();
    this.startTime = Instant.now();
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(!hasCommitted, "Transaction has already committed changes");
    txByTable.forEach((key, value) -> updatesByTable.put(key, value.pendingUpdates()));

    try {
      if (IsolationLevel.SERIALIZABLE_WITH_FIXED_READS == isolationLevel
          || IsolationLevel.SERIALIZABLE_WITH_LOADING_READS == isolationLevel) {
        // make sure tables that are participating in the TX haven't been updated externally
        for (TableIdentifier affectedTable : updatesByTable.keySet()) {
          if (((BaseTable) origin.loadTable(affectedTable))
                  .operations()
                  .current()
                  .lastUpdatedMillis()
              >= startTime.toEpochMilli()) {
            throw new ValidationException(
                "Found updates to table %s at isolation level %s",
                affectedTable.name(), isolationLevel.name());
          }
        }
      }

      // this is effectively SNAPSHOT isolation where we make sure that no conflicting updates have
      // been performed
      Tasks.foreach(updatesByTable.values())
          .run(pendingUpdates -> pendingUpdates.forEach(PendingUpdate::commit));
      Tasks.foreach(txByTable.values()).run(Transaction::commitTransaction);
      hasCommitted = true;
    } catch (CommitStateUnknownException e) {
      throw e;
    } catch (RuntimeException e) {
      rollback();
      throw e;
    }
  }

  @Override
  public Catalog asCatalog() {
    return new AsTransactionalCatalog();
  }

  @Override
  public void rollback() {
    Tasks.foreach(txByTable.values()).run(Transaction::rollback);
  }

  private Optional<Table> txTable(TableIdentifier identifier) {
    if (txByTable.containsKey(identifier)) {
      return Optional.ofNullable(txByTable.get(identifier).table());
    }
    return Optional.empty();
  }

  private Transaction txForTable(Table table) {
    return txByTable.computeIfAbsent(
        TableIdentifier.parse(table.name()),
        k -> {
          TableOperations operations = ((HasTableOperations) table).operations();
          return new BaseTransaction(
              table.name(), operations, TransactionType.SIMPLE, operations.refresh());
        });
  }

  @Override
  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  @Override
  public UpdateSchema updateSchema(Table table) {
    return txForTable(table).updateSchema();
  }

  @Override
  public UpdatePartitionSpec updateSpec(Table table) {
    return txForTable(table).updateSpec();
  }

  @Override
  public UpdateProperties updateProperties(Table table) {
    return txForTable(table).updateProperties();
  }

  @Override
  public ReplaceSortOrder replaceSortOrder(Table table) {
    return txForTable(table).replaceSortOrder();
  }

  @Override
  public UpdateLocation updateLocation(Table table) {
    return txForTable(table).updateLocation();
  }

  @Override
  public AppendFiles newAppend(Table table) {
    return txForTable(table).newAppend();
  }

  @Override
  public AppendFiles newFastAppend(Table table) {
    return txForTable(table).newFastAppend();
  }

  @Override
  public RewriteFiles newRewrite(Table table) {
    return txForTable(table).newRewrite();
  }

  @Override
  public RewriteManifests rewriteManifests(Table table) {
    return txForTable(table).rewriteManifests();
  }

  @Override
  public OverwriteFiles newOverwrite(Table table) {
    return txForTable(table).newOverwrite();
  }

  @Override
  public RowDelta newRowDelta(Table table) {
    return txForTable(table).newRowDelta();
  }

  @Override
  public ReplacePartitions newReplacePartitions(Table table) {
    return txForTable(table).newReplacePartitions();
  }

  @Override
  public DeleteFiles newDelete(Table table) {
    return txForTable(table).newDelete();
  }

  @Override
  public UpdateStatistics updateStatistics(Table table) {
    return txForTable(table).updateStatistics();
  }

  @Override
  public ExpireSnapshots expireSnapshots(Table table) {
    return txForTable(table).expireSnapshots();
  }

  @Override
  public ManageSnapshots manageSnapshots(Table table) {
    return txForTable(table).manageSnapshots();
  }

  public class AsTransactionalCatalog extends TransactionalCatalog {
    @Override
    public Table loadTable(TableIdentifier identifier) {
      Table table =
          BaseCatalogTransaction.this
              .txTable(identifier)
              .orElseGet(() -> origin.loadTable(identifier));
      Preconditions.checkArgument(table instanceof BaseTable);

      if (IsolationLevel.SERIALIZABLE_WITH_FIXED_READS == isolationLevel) {
        // load table using the snapshotId as of TX start time
        Long snapshotId =
            SnapshotUtil.snapshotIdAsOfTimeOptional(table, startTime.toEpochMilli()).orElse(null);
        table = new SnapshotTable((BaseTable) table, snapshotId);
      }

      return table;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      Optional<Table> txTable = BaseCatalogTransaction.this.txTable(tableIdentifier);
      if (txTable.isPresent()) {
        return ((TransactionTable) txTable.get()).operations();
      }
      return origin.newTableOps(tableIdentifier);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return origin.defaultWarehouseLocation(tableIdentifier);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return origin.listTables(namespace);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return origin.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      origin.renameTable(from, to);
    }

    @Override
    public Set<IsolationLevel> supportedIsolationLevels() {
      return origin.supportedIsolationLevels();
    }
  }

  public static class SnapshotTable extends BaseTable {
    private final BaseTable baseTable;
    private final Long snapshotId;
    private final Snapshot currentSnapshot;

    private SnapshotTable(BaseTable table, Long snapshotId) {
      super(table.operations(), table.name());
      this.baseTable = table;
      this.snapshotId = snapshotId;
      this.currentSnapshot = baseTable.snapshot(snapshotId);
    }

    @Override
    public Schema schema() {
      return SnapshotUtil.schemaFor(baseTable, snapshotId, null);
    }

    @Override
    public Snapshot currentSnapshot() {
      return currentSnapshot;
    }
  }
}
