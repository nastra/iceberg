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

package org.apache.iceberg.catalog;

import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/**
 * Identifies a table in an iceberg catalog.
 */
public class TableIdentifier {

  private static final Splitter DOT = Splitter.on('.');
  private static final String AT = "@";

  private final Namespace namespace;
  private final String name;

  @Nullable
  private final String referenceName;

  public static TableIdentifier of(String... names) {
    Preconditions.checkArgument(names != null, "Cannot create table identifier from null array");
    Preconditions.checkArgument(names.length > 0, "Cannot create table identifier without a table name");
    String simpleName = names[names.length - 1];
    if (simpleName.contains(AT)) {
      String[] split = simpleName.split(AT);
      return new TableIdentifier(Namespace.of(Arrays.copyOf(names, names.length - 1)), split[0], split[1]);
    }
    return new TableIdentifier(Namespace.of(Arrays.copyOf(names, names.length - 1)), simpleName);
  }

  public static TableIdentifier of(Namespace namespace, String name) {
    return new TableIdentifier(namespace, name);
  }

  public static TableIdentifier of(Namespace namespace, String name, String referenceName) {
    return new TableIdentifier(namespace, name, referenceName);
  }

  public static TableIdentifier parse(String identifier) {
    Preconditions.checkArgument(identifier != null, "Cannot parse table identifier: null");
    Iterable<String> parts = DOT.split(identifier);
    return TableIdentifier.of(Iterables.toArray(parts, String.class));
  }

  private TableIdentifier(Namespace namespace, String name) {
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Invalid table name: null or empty");
    Preconditions.checkArgument(namespace != null, "Invalid Namespace: null");
    this.namespace = namespace;
    this.name = name;
    this.referenceName = null;
  }

  private TableIdentifier(Namespace namespace, String name, String referenceName) {
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Invalid table name: null or empty");
    Preconditions.checkArgument(namespace != null, "Invalid Namespace: null");
    Preconditions.checkArgument(
        referenceName != null && !referenceName.isEmpty(), "Invalid reference name: null or empty");
    this.namespace = namespace;
    this.name = name;
    this.referenceName = referenceName;
  }

  /**
   * Whether the namespace is empty.
   * @return true if the namespace is not empty, false otherwise
   */
  public boolean hasNamespace() {
    return !namespace.isEmpty();
  }

  /**
   * Returns the identifier namespace.
   */
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Whether the identifier has a reference name (which is either a Branch or a Tag).
   * @return true if the table identifier has a reference name (which is either a Branch or a Tag).
   */
  public boolean hasReferenceName() {
    return null != referenceName;
  }

  /**
   * Returns the identifier name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The reference name (which is either a Branch or a Tag) of the table identifier.
   */
  @Nullable
  public String referenceName() {
    return referenceName;
  }

  public TableIdentifier toLowerCase() {
    String[] newLevels = Arrays.stream(namespace().levels())
        .map(String::toLowerCase)
        .toArray(String[]::new);
    String newName = name().toLowerCase();
    // the reference name needs to stay the same, so we won't apply 'toLowerCase' to it
    if (hasReferenceName()) {
      return TableIdentifier.of(Namespace.of(newLevels), newName, referenceName());
    }
    return TableIdentifier.of(Namespace.of(newLevels), newName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableIdentifier that = (TableIdentifier) o;
    return Objects.equals(namespace, that.namespace) && Objects.equals(name, that.name) &&
        Objects.equals(referenceName, that.referenceName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name, referenceName);
  }

  @Override
  public String toString() {
    String tableWithRef = hasReferenceName() ? name + AT + referenceName : name;
    if (hasNamespace()) {
      return namespace.toString() + "." + tableWithRef;
    } else {
      return tableWithRef;
    }
  }
}
