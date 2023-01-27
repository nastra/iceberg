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
}
