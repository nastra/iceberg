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

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SerializableSet;

/** Utility class that allows combining two given {@link MetricsReporter} instances. */
public class MetricsReporters {
  private MetricsReporters() {}

  public static MetricsReporter combine(MetricsReporter first, MetricsReporter second) {
    if (null == first) {
      return second;
    } else if (null == second || first == second) {
      return first;
    }

    if (first instanceof CompositeMetricsReporter && second instanceof CompositeMetricsReporter) {
      return combineComposites((CompositeMetricsReporter) first, (CompositeMetricsReporter) second);
    }

    if (first instanceof CompositeMetricsReporter) {
      return combineWithSimpleReporter((CompositeMetricsReporter) first, second);
    }

    if (second instanceof CompositeMetricsReporter) {
      return combineWithSimpleReporter((CompositeMetricsReporter) second, first);
    }

    return combineSimpleReporters(first, second);
  }

  private static CompositeMetricsReporter combineSimpleReporters(
      MetricsReporter first, MetricsReporter second) {
    Set<MetricsReporter> result = Sets.newIdentityHashSet();
    result.add(first);
    result.add(second);
    return ImmutableCompositeMetricsReporter.builder()
        .metricsReporters(SerializableSet.copyOf(result))
        .build();
  }

  private static CompositeMetricsReporter combineComposites(
      CompositeMetricsReporter first, CompositeMetricsReporter second) {
    Set<MetricsReporter> result = Sets.newIdentityHashSet();
    result.addAll(first.metricsReporters());
    result.addAll(second.metricsReporters());

    return ImmutableCompositeMetricsReporter.builder()
        .metricsReporters(SerializableSet.copyOf(result))
        .build();
  }

  private static CompositeMetricsReporter combineWithSimpleReporter(
      CompositeMetricsReporter first, MetricsReporter second) {
    Set<MetricsReporter> result = Sets.newIdentityHashSet();
    result.addAll(first.metricsReporters());
    result.add(second);

    return ImmutableCompositeMetricsReporter.builder()
        .metricsReporters(SerializableSet.copyOf(result))
        .build();
  }
}
