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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.iceberg.util.SerializableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTMetricsReporter implements MetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(RESTMetricsReporter.class);
  private static final Splitter DOT = Splitter.on('.');

  private transient volatile RESTClient client;
  private final ResourcePaths paths;
  private final Map<String, String> headers;
  private final SerializableFunction<Map<String, String>, RESTClient> clientBuilder;
  private final Map<String, String> properties;

  public RESTMetricsReporter(
      RESTClient client,
      ResourcePaths paths,
      Map<String, String> headers,
      SerializableFunction<Map<String, String>, RESTClient> clientBuilder,
      Map<String, String> properties) {
    this.client = client;
    this.paths = paths;
    this.headers = SerializableMap.copyOf(headers);
    this.clientBuilder = clientBuilder;
    this.properties = SerializableMap.copyOf(properties);
  }

  @Override
  public void report(MetricsReport report) {
    if (null == report) {
      LOG.warn("Received invalid metrics report: null");
      return;
    }

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
        client()
            .post(
                paths.metrics(tableIdentifier),
                ReportMetricsRequest.of(report),
                null,
                headers,
                ErrorHandlers.defaultErrorHandler());
      }
    } catch (Exception e) {
      LOG.warn("Failed to report metrics to REST endpoint for table {}", tableIdentifier, e);
    }
  }

  private RESTClient client() {
    // we lazy init the client in case RESTMetricsReporter was deserialized
    if (null == client) {
      synchronized (RESTMetricsReporter.class) {
        if (null == client) {
          client = clientBuilder.apply(properties);
        }
      }
    }

    return client;
  }

  private TableIdentifier identifierWithoutCatalog(String tableNameWithCatalog) {
    List<String> split = DOT.splitToList(tableNameWithCatalog);
    if (split.size() > 2) {
      return TableIdentifier.parse(tableNameWithCatalog.replace(split.get(0) + ".", ""));
    }

    return TableIdentifier.parse(tableNameWithCatalog);
  }
}
