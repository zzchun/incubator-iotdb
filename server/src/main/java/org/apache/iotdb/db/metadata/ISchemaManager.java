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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

public interface ISchemaManager {

  /**
   * init the schema manager.
   * This function will be called after a manamger instance is created.
   * 1. NOTICE that the implementation of this method should be thread-safety.
   * 2. the implementataion should be idempotent (means the method can be called multiple times)
   */
  void init();

  /**
   * clear the schema in this manager.
   * all schema in memory and on disk should be cleared
   */
  void clear();

  /**
   * execute the cmd to operate schema
   * @param cmd
   * TODO command format:
   * @throws IOException if saving the schema failed on disk
   * @throws MetadataException if cmd has error, or the cmd is conflict with existing schema.
   */
  void operation(String cmd) throws IOException, MetadataException;

  /**
   * create a new time series.
   * @param plan the create time series plan.
   * @throws MetadataException if the plan has error or it is conflict with existing schema.
   */
  void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException;

  /**
   * create a new time series.
   * @param path the full path of the time series
   * @param dataType the data type of the series
   * @param encoding the encoding method of the series
   * @param compressor the compression method of the series
   * @param props the customized properties of the series
   * @throws MetadataException if something has error or it is conflict with existing schema.
   */
  void createTimeseries(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws MetadataException;

  /**
   * Delete all timeseries under the given path, may cross different storage group
   *
   * @param prefixPath path to be deleted, could be root or a prefix path or a full path
   * @return the deletion failed Timeseries, which are contacted by ","
   */
  String deleteTimeseries(String prefixPath) throws MetadataException;

  /**
   * add a new storage group
   * @param storageGroup the full path of the storage group
   * @throws MetadataException if the path has error or is conflict with existing storage groups.
   */
  void setStorageGroup(String storageGroup) throws MetadataException;

  /**
   * delete given storage groups
   * @param storageGroups  full paths of storage groups
   * @throws MetadataException if deletion failed (e.g., the sg does not exist)
   */
  void deleteStorageGroups(List<String> storageGroups) throws MetadataException;

  /**
   * get the data type of a time series
   * @param path full path of a series
   * @return data type of the time series
   * @throws MetadataException query failed or the time series does not exist
   */
  TSDataType getSeriesType(String path) throws MetadataException;

  /**
   * get the detail schema of given time series
   * @param deviceId the prefix path of the given time series
   * @param measurements the suffix paths of the given time series (does not contains ".")
   * @return the detail schemas.
   * @throws MetadataException
   */
  MeasurementSchema[] getSchemas(String deviceId, String[] measurements)
      throws MetadataException;

  /**
   * get all devices whose prefix paths match the prefixPath
   * @param prefixPath a prefix path, must start with root. The prefix path can contain *.
   * @return all devices whose prefix paths match the prefixPath
   * @throws MetadataException
   */
  Set<String> getDevices(String prefixPath) throws MetadataException;

  /**
   * Get all paths from the given level which have the given prefix path.
   *
   * @param prefixPath a prefix path that starts with root and does not allow having *.
   * @param nodeLevel  the level of the path that will be returned. The level should >= the level of
   *                   the given prefix path.
   * @return A List instance which stores all node at given level
   */
  List<String> getNodesList(String prefixPath, int nodeLevel) throws MetadataException;

  /**
   * given a prefix path or full path, find which storage group it belongs to
   * @param path a prefix path or full path
   * @return
   * @throws StorageGroupNotSetException
   */
  String getStorageGroupName(String path) throws StorageGroupNotSetException;

  /**
   * @return all storage group names in the system
   */
  List<String> getAllStorageGroupNames();

  /**
   *
   * @return all storage group nodes in the system
   */
  List<StorageGroupMNode> getAllStorageGroupNodes();

  /**
   * get all full paths which match the given prefix Path.
   * @param prefixPath a prefix path which starts with root, and can contain *.
   * @return all full paths
   * @throws MetadataException
   */
  List<String> getAllTimeseriesName(String prefixPath) throws MetadataException;

  /**
   * get all full paths which match the given prefix Path.
   * @param prefixPath a prefix path which starts with root, and can contain *.
   * @return all full paths
   * @throws MetadataException
   */
  List<Path> getAllTimeseriesPath(String prefixPath) throws MetadataException;

  /**
   * get the number of all full paths which match the given prefix Path.
   * @param prefixPath a prefix path which starts with root, and can contain *.
   * @return
   * @throws MetadataException
   */
  int getAllTimeseriesCount(String prefixPath) throws MetadataException;

  /**
   * get the number of all  paths which match the given prefix Path, and whose level equal to the
   * given level
   * @param prefixPath a prefix path which starts with root, and can contain *.
   * @param level a given level, must >= the level ofthe given prefixPath
   * @return
   * @throws MetadataException
   */
  int getNodesCountInGivenLevel(String prefixPath, int level) throws MetadataException;

  List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException;

  /**
   * get the detail schema of given time series
   * @param device the prefix path of the given time series
   * @param measurement the suffix path of the given time series (does not contains ".")
   * @return the detail schema.
   * @throws MetadataException
   */
  MeasurementSchema getSeriesSchema(String device, String measurement)
          throws MetadataException;

  Set<String> getChildNodePathInNextLevel(String path) throws MetadataException;

  boolean isPathExist(String path);

  MNode getNodeByPath(String path) throws MetadataException;

  StorageGroupMNode getStorageGroupNode(String path) throws MetadataException;

  MNode getDeviceNodeWithAutoCreateAndReadLock(
      String path, boolean autoCreateSchema, int sgLevel) throws MetadataException;

  MNode getDeviceNodeWithAutoCreateAndReadLock(String path) throws MetadataException;

  MNode getDeviceNode(String path) throws MetadataException;

  String getDeviceId(String path);

  MNode getChild(MNode parent, String child);

  String getMetadataInString();

  @TestOnly
  void setMaxSeriesNumberAmongStorageGroup(long maxSeriesNumberAmongStorageGroup);

  long getMaximalSeriesNumberAmongStorageGroups();

  void setTTL(String storageGroup, long dataTTL) throws MetadataException, IOException;

  Map<String, Long> getStorageGroupsTTL();

  void changeOffset(String path, long offset) throws MetadataException;

  void changeAlias(String path, String alias) throws MetadataException;

  void upsertTagsAndAttributes(String alias, Map<String, String> tagsMap,
      Map<String, String> attributesMap, String fullPath) throws MetadataException, IOException;

  void addAttributes(Map<String, String> attributesMap, String fullPath)
          throws MetadataException, IOException;

  void addTags(Map<String, String> tagsMap, String fullPath)
              throws MetadataException, IOException;

  void dropTagsOrAttributes(Set<String> keySet, String fullPath)
                  throws MetadataException, IOException;

  void setTagsOrAttributesValue(Map<String, String> alterMap, String fullPath)
                      throws MetadataException, IOException;

  void renameTagOrAttributeKey(String oldKey, String newKey, String fullPath)
                          throws MetadataException, IOException;

  void collectTimeseriesSchema(MNode startingNode,
      Collection<TimeseriesSchema> timeseriesSchemas);

  void collectMeasurementSchema(MNode startingNode,
      Collection<MeasurementSchema> timeseriesSchemas);

  void collectSeries(String startingPath, List<MeasurementSchema> measurementSchemas);

  Map<String, String> determineStorageGroup(String path) throws IllegalPathException;

  void cacheMeta(String path, MeasurementMeta meta);

  void updateLastCache(String seriesPath, TimeValuePair timeValuePair,
      boolean highPriorityUpdate, Long latestFlushedTime,
      MeasurementMNode node);

  TimeValuePair getLastCache(String seriesPath);

  void createMTreeSnapshot();

  MeasurementSchema[] getSeriesSchemasAndReadLockDevice(String deviceId,
      String[] measurementList, InsertPlan plan) throws MetadataException;

  void unlockDeviceReadLock(String deviceId);
}
