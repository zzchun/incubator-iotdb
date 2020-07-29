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
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public interface IMeasurementMNode extends ISchemaNode{

  /**
   * deserialize MeasuremetMNode from string array
   *
   * @param nodeInfo node information array. For example: "2,s0,speed,2,2,1,year:2020;month:jan;,-1,0"
   * representing: [0] nodeType [1] name [2] alias [3] TSDataType.ordinal() [4] TSEncoding.ordinal()
   * [5] CompressionType.ordinal() [6] props [7] offset [8] children size
   */
  static IMeasurementMNode deserializeFrom(String[] nodeInfo) {
    String name = nodeInfo[1];
    String alias = nodeInfo[2].equals("") ? null : nodeInfo[2];
    Map<String, String> props = new HashMap<>();
    if (!nodeInfo[6].equals("")) {
      for (String propInfo : nodeInfo[6].split(";")) {
        props.put(propInfo.split(":")[0], propInfo.split(":")[1]);
      }
    }
    MeasurementSchema schema = new MeasurementSchema(name,
        TSDataType.deserialize(Short.valueOf(nodeInfo[3])),
        TSEncoding.deserialize(Short.valueOf(nodeInfo[4])),
        CompressionType.deserialize(Short.valueOf(nodeInfo[5])), props);
    IMeasurementMNode node = new MeasurementMNode(null, name, schema, alias);
    node.setOffset(Long.valueOf(nodeInfo[7]));

    return node;
  }

  MeasurementSchema getSchema();

  TimeValuePair getCachedLast();

  void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime);

  String getFullPath();

  void resetCache();

  long getOffset();

  void setOffset(long offset);

  String getAlias();

  void setAlias(String alias);

  void setSchema(MeasurementSchema schema);

  void serializeTo(BufferedWriter bw) throws IOException;
}
