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
package org.apache.iceberg.rest.requests;

import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;

public class ScanReportRequest implements RESTRequest {

  private ScanReport scanReport;

  @SuppressWarnings("unused")
  public ScanReportRequest() {
    // Needed for Jackson Deserialization.
  }

  private ScanReportRequest(ScanReport scanReport) {
    this.scanReport = scanReport;
    validate();
  }

  public ScanReport scanReport() {
    return scanReport;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("scanReport", scanReport).toString();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(null != scanReport, "Invalid scan report: null");
  }

  public static ScanReportRequest.Builder builder() {
    return new ScanReportRequest.Builder();
  }

  public static class Builder {
    private ScanReport scanReport;

    private Builder() {}

    public Builder fromScanReport(ScanReport newScanReport) {
      this.scanReport = newScanReport;
      return this;
    }

    public ScanReportRequest build() {
      Preconditions.checkArgument(null != scanReport, "Invalid scan report: null");
      return new ScanReportRequest(scanReport);
    }
  }
}
