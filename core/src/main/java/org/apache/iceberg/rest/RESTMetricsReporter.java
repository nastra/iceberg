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
package org.apache.iceberg.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTMetricsReporter implements MetricsReporter, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RESTMetricsReporter.class);
  private static final Splitter DOT = Splitter.on('.');

  private RESTClient client;
  private ResourcePaths paths;
  private Map<String, String> properties;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.client =
        HTTPClient.builder(catalogProperties)
            .uri(catalogProperties.get(CatalogProperties.URI))
            .build();
    this.paths = ResourcePaths.forCatalogProperties(catalogProperties);
    this.properties = ImmutableMap.copyOf(catalogProperties);
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public void report(MetricsReport report) {
    TableIdentifier tableIdentifier = null;
    try {
      // we need to derive the namespace + table name from the report itself
      if (report instanceof ScanReport) {
        ScanReport scanReport = (ScanReport) report;
        tableIdentifier = identifierWithoutCatalog(scanReport.tableName());
      } else if (report instanceof CommitReport) {
        CommitReport commitReport = (CommitReport) report;
        tableIdentifier = identifierWithoutCatalog(commitReport.tableName());
      } else {
        LOG.warn("Failed to derive namespace/table name from metrics report {}", report);
      }

      if (null != tableIdentifier) {
        // TODO: should this require authorization headers to send a metrics report?
        client.post(
            paths.metrics(tableIdentifier),
            ReportMetricsRequest.of(report),
            null,
            ImmutableMap.of(),
            ErrorHandlers.defaultErrorHandler());
      }
    } catch (Exception e) {
      LOG.warn("Failed to report metrics to REST endpoint for table {}", tableIdentifier, e);
    }
  }

  private TableIdentifier identifierWithoutCatalog(String tableNameWithCatalog) {
    List<String> split = DOT.splitToList(tableNameWithCatalog);
    if (split.size() > 2) {
      return TableIdentifier.parse(tableNameWithCatalog.replace(split.get(0) + ".", ""));
    }

    return TableIdentifier.parse(tableNameWithCatalog);
  }

  @Override
  public void close() {
    if (null != client) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.warn("Failed to close REST client", e);
      }
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }
}
