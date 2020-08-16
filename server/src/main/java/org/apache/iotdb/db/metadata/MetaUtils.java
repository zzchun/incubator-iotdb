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
package org.apache.iotdb.db.metadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;

public class MetaUtils {

  public static final String PATH_SEPARATOR = "\\.";

  private MetaUtils() {

  }

  public static String[] getNodeNames(String path) {
    String[] nodeNames;
    if (path.contains("\"") || path.contains("'")) {
      // e.g., root.sg.d1."s1.int"  ->  root.sg.d1, s1.int
      String[] measurementDeviceNode = path.trim().replace("'", "\"").split("\"");
      // s1.int
      String measurement = measurementDeviceNode[1];
      // root.sg.d1 -> root, sg, d1
      String[] deviceNodeName = measurementDeviceNode[0].split(PATH_SEPARATOR);
      int nodeNumber = deviceNodeName.length + 1;
      nodeNames = new String[nodeNumber];
      System.arraycopy(deviceNodeName, 0, nodeNames, 0, nodeNumber - 1);
      // nodeNames = [root, sg, d1, s1.int]
      nodeNames[nodeNumber - 1] = measurement;
    } else {
      nodeNames = path.split(PATH_SEPARATOR);
    }
    return nodeNames;
  }

  static String getNodeRegByIdx(int idx, String[] nodes) {
    return idx >= nodes.length ? PATH_WILDCARD : nodes[idx];
  }

  public static String getPathByDetachedPath(String[] nodes) {
    StringBuilder s = new StringBuilder(nodes[0]);
    for(int i = 1; i < nodes.length; i++) {
      s.append(nodes[i]);
    }
    return s.toString();
  }

  /**
   *
   * @param path the path will split. ex, root.ln
   * @return string array. ex, [root, ln]
   * @throws IllegalPathException if path isn't correct, the exception will throw
   */
  public static String[] splitPathToDetachedPath(String path) throws IllegalPathException {
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == IoTDBConstant.PATH_SEPARATOR) {
        nodes.add(path.substring(startIndex, i));
        startIndex = i + 1;
      } else if (path.charAt(i) == '"') {
        int endIndex = path.indexOf('"', i + 1);
        if (endIndex != -1 && (endIndex == path.length() - 1 || path.charAt(endIndex + 1) == '.')) {
          nodes.add(path.substring(startIndex, endIndex + 1));
          i = endIndex + 1;
          startIndex = endIndex + 2;
        } else {
          throw new IllegalPathException("Illegal path: " + path);
        }
      } else if (path.charAt(i) == '\'') {
        throw new IllegalPathException("Illegal path with single quote: " + path);
      }
    }
    if (startIndex <= path.length() - 1) {
      nodes.add(path.substring(startIndex));
    }
    return nodes.toArray(new String[0]);
  }

  /**
   * Get storage group name when creating schema automatically is enable
   *
   * e.g., path = root.a.b.c and level = 1, return root.a
   *
   * @param path path
   * @param level level
   */
  static PartialPath getStorageGroupNameByLevel(PartialPath path, int level) throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= level || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      throw new IllegalPathException(path.toString());
    }
    String[] storageGroupNodes = new String[level];
    System.arraycopy(nodeNames, 0, storageGroupNodes, 0, level);
    return new PartialPath(storageGroupNodes);
  }
}
