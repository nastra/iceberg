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
package org.apache.iceberg.catalog;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;

public interface CatalogTransaction {

  enum IsolationLevel {

    /**
     * Table can change underneath but still have to pass normal table validation. Loading table
     * data when it's referenced.
     */
    SNAPSHOT,

    /**
     * All tables participating in the transaction must be in the same state when committing
     * compared to when the transaction started. Loading table data when it is referenced.
     */
    SERIALIZABLE_WITH_LOADING_READS,

    /**
     * All tables participating in the transaction must be in the same state when committing
     * compared to when the transaction started. Loading table data as of TX start time.
     */
    SERIALIZABLE_WITH_FIXED_READS,
  }

  void commitTransaction();

  void rollback();

  Catalog asCatalog();

  /**
   * Return the current {@link IsolationLevel} for this transaction.
   *
   * @return The {@link IsolationLevel} for this transaction.
   */
  IsolationLevel isolationLevel();

  /**
   * Create a new {@link UpdateSchema} to alter the columns of this table.
   *
   * @return a new {@link UpdateSchema}
   */
  UpdateSchema updateSchema(Table table);

  /**
   * Create a new {@link UpdatePartitionSpec} to alter the partition spec of this table.
   *
   * @return a new {@link UpdatePartitionSpec}
   */
  UpdatePartitionSpec updateSpec(Table table);

  /**
   * Create a new {@link UpdateProperties} to update table properties.
   *
   * @return a new {@link UpdateProperties}
   */
  UpdateProperties updateProperties(Table table);

  /**
   * Create a new {@link ReplaceSortOrder} to set a table sort order and commit the change.
   *
   * @return a new {@link ReplaceSortOrder}
   */
  ReplaceSortOrder replaceSortOrder(Table table);

  /**
   * Create a new {@link UpdateLocation} to update table location.
   *
   * @return a new {@link UpdateLocation}
   */
  UpdateLocation updateLocation(Table table);

  /**
   * Create a new {@link AppendFiles append API} to add files to this table.
   *
   * @return a new {@link AppendFiles}
   */
  AppendFiles newAppend(Table table);

  /**
   * Create a new {@link AppendFiles append API} to add files to this table.
   *
   * <p>Using this method signals to the underlying implementation that the append should not
   * perform extra work in order to commit quickly. Fast appends are not recommended for normal
   * writes because the fast commit may cause split planning to slow down over time.
   *
   * <p>Implementations may not support fast appends, in which case this will return the same
   * appender as {@link #newAppend(Table)}.
   *
   * @return a new {@link AppendFiles}
   */
  AppendFiles newFastAppend(Table table);

  /**
   * Create a new {@link RewriteFiles rewrite API} to replace files in this table.
   *
   * @return a new {@link RewriteFiles}
   */
  RewriteFiles newRewrite(Table table);

  /**
   * Create a new {@link RewriteManifests rewrite manifests API} to replace manifests for this
   * table.
   *
   * @return a new {@link RewriteManifests}
   */
  RewriteManifests rewriteManifests(Table table);

  /**
   * Create a new {@link OverwriteFiles overwrite API} to overwrite files by a filter expression.
   *
   * @return a new {@link OverwriteFiles}
   */
  OverwriteFiles newOverwrite(Table table);

  /**
   * Create a new {@link RowDelta row-level delta API} to remove or replace rows in existing data
   * files.
   *
   * @return a new {@link RowDelta}
   */
  RowDelta newRowDelta(Table table);

  /**
   * Not recommended: Create a new {@link ReplacePartitions replace partitions API} to dynamically
   * overwrite partitions in the table with new data.
   *
   * <p>This is provided to implement SQL compatible with Hive table operations but is not
   * recommended. Instead, use the {@link OverwriteFiles overwrite API} to explicitly overwrite
   * data.
   *
   * @return a new {@link ReplacePartitions}
   */
  ReplacePartitions newReplacePartitions(Table table);

  /**
   * Create a new {@link DeleteFiles delete API} to replace files in this table.
   *
   * @return a new {@link DeleteFiles}
   */
  DeleteFiles newDelete(Table table);

  /**
   * Create a new {@link UpdateStatistics update table statistics API} to add or remove statistics
   * files in this table.
   *
   * @return a new {@link UpdateStatistics}
   */
  UpdateStatistics updateStatistics(Table table);

  /**
   * Create a new {@link ExpireSnapshots expire API} to manage snapshots in this table.
   *
   * @return a new {@link ExpireSnapshots}
   */
  ExpireSnapshots expireSnapshots(Table table);

  ManageSnapshots manageSnapshots(Table table);
}
