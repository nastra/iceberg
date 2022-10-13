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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.ImmutableNamespaceWithProperties;
import org.apache.iceberg.rest.NamespaceWithProperties;
import org.apache.iceberg.rest.NamespaceWithPropertiesParser;
import org.apache.iceberg.util.JsonUtil;

public class CreateNamespaceRequestParser {

  private CreateNamespaceRequestParser() {}

  public static String toJson(CreateNamespaceRequest request) {
    return toJson(request, false);
  }

  public static String toJson(CreateNamespaceRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(CreateNamespaceRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid namespace creation request: null");

    NamespaceWithPropertiesParser.toJson(
        ImmutableNamespaceWithProperties.builder()
            .namespace(request.namespace())
            .properties(request.properties())
            .build(),
        gen);
  }

  public static CreateNamespaceRequest fromJson(String json) {
    return JsonUtil.parse(json, CreateNamespaceRequestParser::fromJson);
  }

  public static CreateNamespaceRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse namespace creation request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse namespace creation request from non-object: %s", json);

    NamespaceWithProperties namespaceWithProperties = NamespaceWithPropertiesParser.fromJson(json);
    return ImmutableCreateNamespaceRequest.newBuilder()
        .namespace(namespaceWithProperties.namespace())
        .properties(namespaceWithProperties.properties())
        .build();
  }
}
