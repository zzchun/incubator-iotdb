/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.mnode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import org.apache.iotdb.db.exception.metadata.DeleteFailedException;

public interface ISchemaNode {

  boolean hasChild(String name);

  void addChild(String name, ISchemaNode child);

  void deleteChild(String name) throws DeleteFailedException;

  void deleteAliasChild(String alias) throws DeleteFailedException;

  ISchemaNode getChild(String name);

  int getLeafCount();

  void addAlias(String alias, ISchemaNode child);

  String getFullPath();

  ISchemaNode getParent();

  void setParent(ISchemaNode parent);

  Map<String, ISchemaNode> getChildren();

  String getName();

  void setName(String name);

  void setChildren(Map<String, ISchemaNode> children);

  void serializeTo(BufferedWriter bw) throws IOException;

  void readLock();

  void readUnlock();

  Lock getWriteLock();

  Lock getReadLock();
}
