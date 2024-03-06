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
package org.apache.iceberg.view;

import java.io.Serializable;
import java.util.UUID;
import org.apache.iceberg.UpdateLocation;

abstract class BaseReadOnlyView implements View, Serializable {

  private final String descriptor;
  private final UUID uuid;

  public BaseReadOnlyView(String descriptor) {
    this.descriptor = descriptor;
    uuid = UUID.randomUUID();
  }

  @Override
  public UpdateViewProperties updateProperties() {
    throw new UnsupportedOperationException(
        "Cannot update properties for a " + descriptor + " view");
  }

  @Override
  public ReplaceViewVersion replaceVersion() {
    throw new UnsupportedOperationException("Cannot replace version for a " + descriptor + " view");
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException("Cannot update location for a " + descriptor + " view");
  }

  @Override
  public UUID uuid() {
    return uuid;
  }
}
