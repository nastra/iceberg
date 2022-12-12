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
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class CommitTransactionRequestParser {
  private static final String TABLE_CHANGES = "table-changes";
  private static final String UPDATES = "updates";
  private static final String REQUIREMENTS = "requirements";
  private static final String IDENTIFIER = "identifier";

  private CommitTransactionRequestParser() {}

  public static String toJson(CommitTransactionRequest request) {
    return toJson(request, false);
  }

  public static String toJson(CommitTransactionRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(CommitTransactionRequest request, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != request, "Invalid commit tx request: null");

    gen.writeStartObject();

    for (CommitTransactionRequest.CommitTableRequest tableChange : request.tableChanges()) {
      gen.writeObjectFieldStart(tableChange.identifier().toString());
      gen.writeFieldName(IDENTIFIER);
      TableIdentifierParser.toJson(tableChange.identifier(), gen);

      gen.writeArrayFieldStart(REQUIREMENTS);
      for (UpdateTableRequest.UpdateRequirement updateRequirement : tableChange.requirements()) {
        UpdateRequirementParser.toJson(updateRequirement, gen);
      }
      gen.writeEndArray();

      gen.writeArrayFieldStart(UPDATES);
      for (MetadataUpdate metadataUpdate : tableChange.updates()) {
        MetadataUpdateParser.toJson(metadataUpdate, gen);
      }
      gen.writeEndArray();

      gen.writeEndObject();
    }

    gen.writeEndObject();
  }

  public static CommitTransactionRequest fromJson(String json) {
    return JsonUtil.parse(json, CommitTransactionRequestParser::fromJson);
  }

  public static CommitTransactionRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse commit tx request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse commit tx request from non-object: %s", json);

    ImmutableCommitTransactionRequest.Builder builder = ImmutableCommitTransactionRequest.builder();
    json.fields()
        .forEachRemaining(
            node -> {
              ImmutableCommitTableRequest.Builder commitTableBuilder =
                  ImmutableCommitTableRequest.builder();
              TableIdentifier identifier =
                  TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, node.getValue()));
              commitTableBuilder.identifier(identifier);

              JsonNode requirements = JsonUtil.get(REQUIREMENTS, node.getValue());
              Preconditions.checkArgument(
                  requirements.isArray(),
                  "Cannot parse requirements from non-array: %s",
                  requirements);
              requirements.forEach(
                  req -> commitTableBuilder.addRequirements(UpdateRequirementParser.fromJson(req)));

              JsonNode updates = JsonUtil.get(UPDATES, node.getValue());
              Preconditions.checkArgument(
                  updates.isArray(), "Cannot parse metadata updates from non-array: %s", updates);

              updates.forEach(
                  update -> commitTableBuilder.addUpdates(MetadataUpdateParser.fromJson(update)));
              builder.addTableChanges(commitTableBuilder.build());
            });

    return builder.build();
  }
}
