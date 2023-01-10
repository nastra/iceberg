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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ViewMetadata implements Serializable {
  static final int SUPPORTED_VIEW_FORMAT_VERSION = 1;

  public abstract int formatVersion();

  public abstract String location();

  public abstract Integer currentSchemaId();

  public abstract List<Schema> schemas();

  public abstract int currentVersionId();

  public abstract List<ViewVersion> versions();

  public abstract List<ViewHistoryEntry> history();

  public abstract Map<String, String> properties();

  public ViewVersion version(int versionId) {
    return versionsById().get(versionId);
  }

  @Value.Lazy
  public ViewVersion currentVersion() {
    return versionsById().get(currentVersionId());
  }

  @Value.Derived
  public Map<Integer, ViewVersion> versionsById() {
    return indexVersions(versions());
  }

  @Value.Derived
  public Map<Integer, Schema> schemasById() {
    return indexSchemas(schemas());
  }

  @Value.Lazy
  public Schema schema() {
    return schemasById().get(currentSchemaId());
  }

  @Value.Default
  int versionHistorySizeToKeep() {
    return PropertyUtil.propertyAsInt(
        properties(),
        ViewProperties.VERSION_HISTORY_SIZE,
        ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);
  }

  @Value.Check
  protected ViewMetadata checkAndNormalize() {
    Preconditions.checkArgument(
        formatVersion() > 0 && formatVersion() <= ViewMetadata.SUPPORTED_VIEW_FORMAT_VERSION,
        "Unsupported format version: %s",
        formatVersion());

    // TODO: should the size of versions() and history() always be equal to currentVersionId()?
    Preconditions.checkArgument(versions().size() > 0, "Invalid view versions: empty");
    Preconditions.checkArgument(history().size() > 0, "Invalid view history: empty");
    Preconditions.checkArgument(schemas().size() > 0, "Invalid schemas: empty");

    Preconditions.checkArgument(
        versionsById().containsKey(currentVersionId()),
        "Cannot find current version %s in view versions: %s",
        currentVersionId(),
        versionsById().keySet());

    Preconditions.checkArgument(
        schemasById().containsKey(currentSchemaId()),
        "Cannot find current schema with id %s in schemas: %s",
        currentSchemaId(),
        schemasById().keySet());

    Preconditions.checkArgument(
        versionHistorySizeToKeep() >= 1,
        "%s must be positive but was %s",
        ViewProperties.VERSION_HISTORY_SIZE,
        versionHistorySizeToKeep());

    if (versions().size() > versionHistorySizeToKeep()) {
      List<ViewVersion> versions =
          versions().subList(versions().size() - versionHistorySizeToKeep(), versions().size());
      List<ViewHistoryEntry> history =
          history().subList(history().size() - versionHistorySizeToKeep(), history().size());
      return ImmutableViewMetadata.builder().from(this).versions(versions).history(history).build();
    }

    return this;
  }

  private static Map<Integer, ViewVersion> indexVersions(List<ViewVersion> versions) {
    ImmutableMap.Builder<Integer, ViewVersion> builder = ImmutableMap.builder();
    for (ViewVersion version : versions) {
      builder.put(version.versionId(), version);
    }

    return builder.build();
  }

  private static Map<Integer, Schema> indexSchemas(List<Schema> schemas) {
    if (schemas == null) {
      return ImmutableMap.of();
    }

    ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
    for (Schema schema : schemas) {
      builder.put(schema.schemaId(), schema);
    }

    return builder.build();
  }
}
