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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class TestCompositeMetricsReporter {

  @Test
  public void nullReporterAndReport() {
    assertThatThrownBy(() -> new CompositeMetricsReporter().register(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metrics reporter: null");

    assertThatThrownBy(() -> new CompositeMetricsReporter().report(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metrics report: null");
  }

  @Test
  public void duplicateReportersWithComposite() {
    MetricsReporter reporter = report -> {};

    CompositeMetricsReporter first = new CompositeMetricsReporter();
    first.register(LoggingMetricsReporter.instance()).register(reporter);

    CompositeMetricsReporter second = new CompositeMetricsReporter();
    second.register(LoggingMetricsReporter.instance()).register(reporter);

    CompositeMetricsReporter composite = new CompositeMetricsReporter();
    composite.register(first).register(second);

    assertThat(composite.metricsReporters())
        .hasSize(2)
        .containsExactlyInAnyOrder(LoggingMetricsReporter.instance(), reporter);
  }

  @Test
  public void duplicateReporters() {
    CompositeMetricsReporter composite = new CompositeMetricsReporter();
    MetricsReporter reporter = report -> {};

    composite.register(LoggingMetricsReporter.instance());
    composite.register(reporter);
    composite.register(LoggingMetricsReporter.instance());
    composite.register(reporter);

    assertThat(composite.metricsReporters())
        .hasSize(2)
        .containsExactlyInAnyOrder(LoggingMetricsReporter.instance(), reporter);
  }

  @Test
  public void loadAndReportWithMultipleMetricsReportersOneFails() {
    CompositeMetricsReporter composite = new CompositeMetricsReporter();
    AtomicInteger counter = new AtomicInteger();
    composite.register(report -> counter.incrementAndGet());
    composite.register(
        report -> {
          throw new RuntimeException("invalid report");
        });
    composite.register(report -> counter.incrementAndGet());

    composite.report(new MetricsReport() {});

    assertThat(composite.metricsReporters()).hasSize(3);
    assertThat(counter.get()).isEqualTo(2);
  }

  @Test
  public void loadAndReportWithMultipleMetricsReporters() {
    CompositeMetricsReporter composite = new CompositeMetricsReporter();
    AtomicInteger counter = new AtomicInteger();
    composite.register(report -> counter.incrementAndGet());
    composite.register(report -> counter.incrementAndGet());

    composite.report(new MetricsReport() {});

    assertThat(composite.metricsReporters()).hasSize(2);
    assertThat(counter.get()).isEqualTo(2);
  }
}
