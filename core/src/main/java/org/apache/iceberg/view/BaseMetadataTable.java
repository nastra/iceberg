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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public abstract class BaseMetadataTable extends BaseReadOnlyView {

  private final View view;
  private final String name;

  public BaseMetadataTable(View view, String name) {
    super("metadata");
    this.view = view;
    this.name = name;
  }

  abstract MetadataType metadataType();

  @Override
  public String name() {
    return name;
  }

  @Override
  public Map<Integer, Schema> schemas() {
    return ImmutableMap.of(ViewMetadata.Builder.INITIAL_SCHEMA_ID, schema());
  }

  @Override
  public ViewVersion currentVersion() {
    return view.currentVersion();
  }

  @Override
  public Iterable<ViewVersion> versions() {
    return view.versions();
  }

  @Override
  public ViewVersion version(int versionId) {
    return view.version(versionId);
  }

  @Override
  public List<ViewHistoryEntry> history() {
    return view.history();
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of();
  }
}
