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
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;

class RESTViewOperations implements ViewOperations {
  enum UpdateType {
    CREATE,
    REPLACE
  }

  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final List<MetadataUpdate> createChanges;
  private final FileIO io;
  private UpdateType updateType;
  private ViewMetadata current;

  RESTViewOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      ViewMetadata current) {
    this(client, path, headers, io, ImmutableList.of(), UpdateType.REPLACE, current);
  }

  RESTViewOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      List<MetadataUpdate> createChanges,
      UpdateType updateType,
      ViewMetadata current) {
    this.client = client;
    this.path = path;
    this.headers = headers;
    this.io = io;
    this.createChanges = createChanges;
    this.updateType = updateType;
    if (updateType == UpdateType.CREATE) {
      this.current = null;
    } else {
      this.current = current;
    }
  }

  @Override
  public ViewMetadata current() {
    return current;
  }

  @Override
  public ViewMetadata refresh() {
    return updateCurrentMetadata(
        client.get(path, LoadViewResponse.class, headers, ErrorHandlers.viewErrorHandler()));
  }

  @Override
  public void commit(ViewMetadata base, ViewMetadata metadata) {
    List<MetadataUpdate> updates =
        ImmutableList.<MetadataUpdate>builder()
            .addAll(createChanges)
            .addAll(metadata.changes())
            .build();

    if (UpdateType.CREATE == updateType) {
      Preconditions.checkState(
          base == null, "Invalid base metadata for create, expected null: %s", base);
    } else {
      Preconditions.checkState(base != null, "Invalid base metadata: null");
    }

    UpdateTableRequest request = UpdateTableRequest.create(null, ImmutableList.of(), updates);

    LoadViewResponse response =
        client.post(
            path, request, LoadViewResponse.class, headers, ErrorHandlers.viewCommitHandler());

    // all future commits should replace the view
    this.updateType = UpdateType.REPLACE;

    updateCurrentMetadata(response);
  }

  public FileIO io() {
    return io;
  }

  private ViewMetadata updateCurrentMetadata(LoadViewResponse response) {
    if (current == null
        || !Objects.equals(current.metadataFileLocation(), response.metadataLocation())) {
      this.current = response.metadata();
    }

    return current;
  }
}
