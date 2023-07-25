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
import java.util.Set;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewMetadata implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ViewMetadata.class);
  public static final int SUPPORTED_VIEW_FORMAT_VERSION = 1;
  private final int formatVersion;
  private final String location;
  private final Integer currentSchemaId;
  private final List<Schema> schemas;
  private final int currentVersionId;
  private List<ViewVersion> versions;
  private List<ViewHistoryEntry> history;
  private final Map<String, String> properties;
  private final List<MetadataUpdate> changes;
  private Map<Integer, ViewVersion> versionsById;
  private Map<Integer, Schema> schemasById;

  private ViewMetadata(
      int formatVersion,
      String location,
      Integer currentSchemaId,
      List<Schema> schemas,
      int currentVersionId,
      List<ViewVersion> versions,
      List<ViewHistoryEntry> history,
      Map<String, String> properties,
      List<MetadataUpdate> changes) {
    Preconditions.checkArgument(null != location, "Invalid location: null");
    Preconditions.checkArgument(null != currentSchemaId, "Invalid schema id: null");
    Preconditions.checkArgument(null != schemas, "Invalid schemas: null");
    Preconditions.checkArgument(null != versions, "Invalid versions: null");
    Preconditions.checkArgument(null != history, "Invalid history: null");
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    Preconditions.checkArgument(null != changes, "Invalid changes: null");
    this.formatVersion = formatVersion;
    this.location = location;
    this.currentSchemaId = currentSchemaId;
    this.schemas = schemas;
    this.currentVersionId = currentVersionId;
    this.versions = versions;
    this.history = history;
    this.properties = properties;
    this.changes = changes;

    Preconditions.checkArgument(
        formatVersion > 0 && formatVersion <= ViewMetadata.SUPPORTED_VIEW_FORMAT_VERSION,
        "Unsupported format version: %s",
        formatVersion());

    Preconditions.checkArgument(versions.size() > 0, "Invalid view versions: empty");
    Preconditions.checkArgument(history.size() > 0, "Invalid view history: empty");
    Preconditions.checkArgument(schemas.size() > 0, "Invalid schemas: empty");

    Preconditions.checkArgument(
        versionsById().containsKey(currentVersionId),
        "Cannot find current version %s in view versions: %s",
        currentVersionId,
        versionsById().keySet());

    Preconditions.checkArgument(
        schemasById().containsKey(currentSchemaId),
        "Cannot find current schema with id %s in schemas: %s",
        currentSchemaId,
        schemasById().keySet());

    int versionHistorySizeToKeep =
        PropertyUtil.propertyAsInt(
            properties(),
            ViewProperties.VERSION_HISTORY_SIZE,
            ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);

    if (versionHistorySizeToKeep <= 0) {
      LOG.warn(
          "{} must be positive but was {}",
          ViewProperties.VERSION_HISTORY_SIZE,
          versionHistorySizeToKeep);
    } else if (versions.size() > versionHistorySizeToKeep) {
      this.versions = versions.subList(versions.size() - versionHistorySizeToKeep, versions.size());
      this.history = history.subList(history.size() - versionHistorySizeToKeep, history.size());
    }
  }

  public int formatVersion() {
    return formatVersion;
  }

  public String location() {
    return location;
  }

  public Integer currentSchemaId() {
    return currentSchemaId;
  }

  public List<Schema> schemas() {
    return schemas;
  }

  public int currentVersionId() {
    return currentVersionId;
  }

  public List<ViewVersion> versions() {
    return versions;
  }

  public List<ViewHistoryEntry> history() {
    return history;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public List<MetadataUpdate> changes() {
    return changes;
  }

  public ViewVersion version(int versionId) {
    return versionsById().get(versionId);
  }

  public ViewVersion currentVersion() {
    return versionsById().get(currentVersionId());
  }

  public Map<Integer, ViewVersion> versionsById() {
    if (null == versionsById) {
      ImmutableMap.Builder<Integer, ViewVersion> builder = ImmutableMap.builder();
      for (ViewVersion version : versions()) {
        builder.put(version.versionId(), version);
      }

      versionsById = builder.build();
    }

    return versionsById;
  }

  public Map<Integer, Schema> schemasById() {
    if (null == schemasById) {
      ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
      for (Schema schema : schemas()) {
        builder.put(schema.schemaId(), schema);
      }

      schemasById = builder.build();
    }

    return schemasById;
  }

  static Builder builder() {
    return new Builder();
  }

  static Builder buildFrom(ViewMetadata base) {
    return new Builder(base);
  }

  public static class Builder {
    private int formatVersion = 1;
    private String location;
    private Integer currentSchemaId;
    private List<Schema> schemas = Lists.newArrayList();
    private int currentVersionId = 1;
    private List<ViewVersion> versions = Lists.newArrayList();
    private List<ViewHistoryEntry> history = Lists.newArrayList();
    private Map<String, String> properties = Maps.newHashMap();
    private List<MetadataUpdate> changes = Lists.newArrayList();

    private Builder() {}

    private Builder(ViewMetadata base) {
      this.formatVersion = base.formatVersion();
      this.location = base.location();
      this.currentSchemaId = base.currentSchemaId();
      this.schemas = Lists.newArrayList(base.schemas());
      this.currentVersionId = base.currentVersionId();
      this.versions = Lists.newArrayList(base.versions());
      this.history = Lists.newArrayList(base.history());
      this.properties = Maps.newHashMap(base.properties());
      this.changes = Lists.newArrayList(base.changes());
    }

    public Builder formatVersion(int newFormatVersion) {
      this.formatVersion = newFormatVersion;
      this.changes.add(new MetadataUpdate.UpgradeFormatVersion(newFormatVersion));
      return this;
    }

    public Builder location(String newLocation) {
      this.location = newLocation;
      this.changes.add(new MetadataUpdate.SetLocation(newLocation));
      return this;
    }

    public Builder currentVersionId(int versionId) {
      this.currentVersionId = versionId;
      this.changes.add(new MetadataUpdate.SetCurrentViewVersion(versionId));
      return this;
    }

    public Builder currentSchemaId(Integer schemaId) {
      this.currentSchemaId = schemaId;
      this.changes.add(new MetadataUpdate.SetCurrentSchema(schemaId));
      return this;
    }

    public Builder addSchema(Schema schema) {
      this.schemas.add(schema);
      this.changes.add(new MetadataUpdate.AddSchema(schema, schema.highestFieldId()));
      return this;
    }

    public Builder addAllSchemas(Iterable<Schema> schemaEntries) {
      for (Schema schema : schemaEntries) {
        this.schemas.add(schema);
        this.changes.add(new MetadataUpdate.AddSchema(schema, schema.highestFieldId()));
      }
      return this;
    }

    public Builder schemas(Iterable<Schema> newSchemas) {
      this.schemas = Lists.newArrayList(newSchemas);
      for (Schema schema : newSchemas) {
        this.changes.add(new MetadataUpdate.AddSchema(schema, schema.highestFieldId()));
      }
      return this;
    }

    public Builder addVersion(ViewVersion version) {
      this.versions.add(version);
      this.changes.add(new MetadataUpdate.AddViewVersion(version));
      return this;
    }

    public Builder addAllVersions(Iterable<ViewVersion> versionEntries) {
      for (ViewVersion version : versionEntries) {
        this.versions.add(version);
        this.changes.add(new MetadataUpdate.AddViewVersion(version));
      }
      return this;
    }

    public Builder versions(Iterable<ViewVersion> newVersions) {
      this.versions = Lists.newArrayList(newVersions);
      for (ViewVersion version : newVersions) {
        this.changes.add(new MetadataUpdate.AddViewVersion(version));
      }
      return this;
    }

    public Builder addHistory(ViewHistoryEntry historyEntry) {
      this.history.add(historyEntry);
      return this;
    }

    public Builder addAllHistory(Iterable<ViewHistoryEntry> historyEntries) {
      for (ViewHistoryEntry entry : historyEntries) {
        this.history.add(entry);
      }
      return this;
    }

    public Builder history(Iterable<ViewHistoryEntry> newHistory) {
      this.history = Lists.newArrayList(newHistory);
      return this;
    }

    public Builder putProperties(String key, String value) {
      this.properties.put(key, value);
      this.changes.add(new MetadataUpdate.SetProperties(ImmutableMap.of(key, value)));
      return this;
    }

    public Builder putAllProperties(Map<String, String> newProperties) {
      this.properties.putAll(newProperties);
      this.changes.add(new MetadataUpdate.SetProperties(newProperties));
      return this;
    }

    public Builder properties(Map<String, String> newProperties) {
      this.properties = newProperties;
      this.changes.add(new MetadataUpdate.SetProperties(newProperties));
      return this;
    }

    public Builder removeProperties(Set<String> propertiesToRemove) {
      if (propertiesToRemove.isEmpty()) {
        return this;
      }

      propertiesToRemove.forEach(this.properties::remove);
      this.changes.add(new MetadataUpdate.RemoveProperties(propertiesToRemove));
      return this;
    }

    public Builder discardChanges() {
      this.changes.clear();
      return this;
    }

    ViewMetadata build() {
      return new ViewMetadata(
          formatVersion,
          location,
          currentSchemaId,
          schemas,
          currentVersionId,
          versions,
          history,
          properties,
          changes);
    }
  }
}
