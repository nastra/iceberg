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

import org.apache.iceberg.metrics.ScanReport.ScanMetrics;

/**
 * This interface defines the basic API for a Table Scan Reporter that can be used to collect and
 * report different metrics during a Table scan.
 */
public interface ScanReporter {

  /**
   * Indicates that a Scan is done by reporting a {@link ScanReport}. A {@link ScanReport} is
   * usually directly derived from a {@link ScanMetrics} instance.
   *
   * @param scanReport The {@link ScanReport} to report.
   */
  void reportScan(ScanReport scanReport);

  /**
   * Creates a new {@link ScanMetrics} instance that can be used during a table scan to add metrics
   * to.
   *
   * @return A new {@link ScanMetrics} instance that can be used during a table scan to add metrics
   *     to.
   */
  ScanMetrics newScanMetrics();

  ScanReporter NOOP =
      new ScanReporter() {
        private final ScanMetrics scanMetrics = ScanMetrics.NOOP;

        @Override
        public void reportScan(ScanReport scanReport) {}

        @Override
        public ScanMetrics newScanMetrics() {
          return scanMetrics;
        }
      };
}
