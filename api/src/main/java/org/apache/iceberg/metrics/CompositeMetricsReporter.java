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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeMetricsReporter implements MetricsReporter, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeMetricsReporter.class);
  private final Map<Class<?>, MetricsReporter> metricsReporters;
  private Map<String, String> properties;

  public CompositeMetricsReporter() {
    metricsReporters = Maps.newConcurrentMap();
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.properties = catalogProperties;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  public CompositeMetricsReporter register(MetricsReporter reporter) {
    Preconditions.checkArgument(null != reporter, "Invalid metrics reporter: null");

    metricsReporters.putIfAbsent(reporter.getClass(), reporter);
    return this;
  }

  @Override
  public void report(MetricsReport report) {
    Preconditions.checkArgument(null != report, "Invalid metrics report: null");

    metricsReporters
        .values()
        .forEach(
            reporter -> {
              try {
                reporter.report(report);
              } catch (Exception e) {
                LOG.warn(
                    "Could not send metrics report of type {} to {}",
                    report.getClass(),
                    reporter.getClass(),
                    e);
              }
            });
  }

  public List<MetricsReporter> metricsReporters() {
    return ImmutableList.copyOf(metricsReporters.values());
  }

  @Override
  public void close() {
    metricsReporters.values().stream()
        .filter(r -> r instanceof Closeable)
        .map(r -> (Closeable) r)
        .forEach(
            reporter -> {
              try {
                reporter.close();
              } catch (Exception e) {
                LOG.warn("Failed to close metrics reporter {}", reporter.getClass(), e);
              }
            });
  }
}
