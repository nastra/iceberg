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

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.util.PropertyUtil;

public class ResourcePaths {
  private static final Joiner SLASH = Joiner.on("/").skipNulls();
  private static final String PREFIX = "prefix";
  public static final String V1_NAMESPACES = "/v1/{prefix}/namespaces";
  public static final String V1_NAMESPACE = "/v1/{prefix}/namespaces/{namespace}";
  public static final String V1_NAMESPACE_PROPERTIES =
      "/v1/{prefix}/namespaces/{namespace}/properties";
  public static final String V1_TABLES = "/v1/{prefix}/namespaces/{namespace}/tables";
  public static final String V1_TABLE = "/v1/{prefix}/namespaces/{namespace}/tables/{table}";
  public static final String V1_TABLE_REGISTER = "/v1/{prefix}/namespaces/{namespace}/register";
  public static final String V1_TABLE_METRICS =
      "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics";
  public static final String V1_TABLE_RENAME = "/v1/{prefix}/tables/rename";
  public static final String V1_TRANSACTIONS_COMMIT = "/v1/{prefix}/transactions/commit";
  public static final String V1_VIEWS = "/v1/{prefix}/namespaces/{namespace}/views";
  public static final String V1_VIEW = "/v1/{prefix}/namespaces/{namespace}/views/{view}";
  public static final String V1_VIEW_RENAME = "/v1/{prefix}/views/rename";

  public static ResourcePaths forCatalogProperties(Map<String, String> properties) {
    return new ResourcePaths(
        properties.get(PREFIX),
        PropertyUtil.propertyAsString(
            properties,
            RESTSessionCatalog.NAMESPACE_SEPARATOR,
            RESTUtil.NAMESPACE_ESCAPED_SEPARATOR));
  }

  public static String config() {
    return "v1/config";
  }

  public static String tokens() {
    return "v1/oauth/tokens";
  }

  private final String prefix;
  private final String namespaceSeparator;

  /**
   * @deprecated since 1.7.0, will be made private in 1.8.0; use {@link
   *     ResourcePaths#forCatalogProperties(Map)} instead.
   */
  @Deprecated
  public ResourcePaths(String prefix) {
    this(prefix, RESTUtil.NAMESPACE_ESCAPED_SEPARATOR);
  }

  private ResourcePaths(String prefix, String namespaceSeparator) {
    this.prefix = prefix;
    this.namespaceSeparator = namespaceSeparator;
  }

  public String namespaces() {
    return SLASH.join("v1", prefix, "namespaces");
  }

  public String namespace(Namespace ns) {
    return SLASH.join("v1", prefix, "namespaces", pathEncode(ns));
  }

  public String namespaceProperties(Namespace ns) {
    return SLASH.join("v1", prefix, "namespaces", pathEncode(ns), "properties");
  }

  public String tables(Namespace ns) {
    return SLASH.join("v1", prefix, "namespaces", pathEncode(ns), "tables");
  }

  public String table(TableIdentifier ident) {
    return SLASH.join(
        "v1",
        prefix,
        "namespaces",
        pathEncode(ident.namespace()),
        "tables",
        RESTUtil.encodeString(ident.name()));
  }

  public String register(Namespace ns) {
    return SLASH.join("v1", prefix, "namespaces", pathEncode(ns), "register");
  }

  public String rename() {
    return SLASH.join("v1", prefix, "tables", "rename");
  }

  public String metrics(TableIdentifier identifier) {
    return SLASH.join(
        "v1",
        prefix,
        "namespaces",
        pathEncode(identifier.namespace()),
        "tables",
        RESTUtil.encodeString(identifier.name()),
        "metrics");
  }

  public String commitTransaction() {
    return SLASH.join("v1", prefix, "transactions", "commit");
  }

  public String views(Namespace ns) {
    return SLASH.join("v1", prefix, "namespaces", pathEncode(ns), "views");
  }

  public String view(TableIdentifier ident) {
    return SLASH.join(
        "v1",
        prefix,
        "namespaces",
        pathEncode(ident.namespace()),
        "views",
        RESTUtil.encodeString(ident.name()));
  }

  public String renameView() {
    return SLASH.join("v1", prefix, "views", "rename");
  }

  private String pathEncode(Namespace ns) {
    return RESTUtil.encodeNamespace(ns, namespaceSeparator);
  }
}
