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

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public enum ServerCapability {
  VIEWS("views"),
  VENDED_CREDENTIALS("vended-credentials"),
  REMOTE_SIGNING("remote-signing"),
  MULTI_TABLE_COMMIT("multi-table-commit");

  private final String value;

  ServerCapability(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static ServerCapability fromValue(String capability) {
    Preconditions.checkArgument(null != capability, "Invalid server capability: null");
    try {
      return ServerCapability.valueOf(capability.toUpperCase(Locale.ENGLISH).replace("-", "_"));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Invalid server capability: %s", capability), e);
    }
  }
}
