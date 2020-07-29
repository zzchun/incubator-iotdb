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
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.ISchemaNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
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
  List<IStorageGroupMNode> getAllStorageGroupNodes();

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

  /**
   * get the direct child nodes of the given path.
   * @param path a prefix of a path, starts with root, can contain *.
   * @return the full paths of the child nodes
   * @throws MetadataException
   */
  Set<String> getChildNodePathInNextLevel(String path) throws MetadataException;

  /**
   * Whether a (prefix) path exist. The path does not contain *.
   * @param path
   * @return
   */
  boolean isPathExist(String path);

  /**
   * convert the given path to a schemaNode structure
   * @param path a path which has no *.
   * @return
   * @throws MetadataException
   */
  ISchemaNode getNodeByPath(String path) throws MetadataException;

  /**
   * check whether the path is a registered Storage group.
   * If so, convert it to IStorageGroupMNode structure.
   * Otherwise, throw StorageGroupNotFound Exception
   * @param path
   * @return
   * @throws MetadataException
   */
  IStorageGroupMNode getStorageGroupNode(String path) throws MetadataException;

  /**
   * get the device node structure, and apply the read lock of the node.
   * If the node does not exist, and autoCreateSchema==true, then create it (and its parents)
   * @param path a path which represents a device.
   * @param autoCreateSchema whether register the device into IoTDB instance.
   * @param sgLevel which level of the path will be considered as the storage group.
   * @return the Node that represents to the device. (node.children() will be measurements)
   * @throws MetadataException
   */
  ISchemaNode getDeviceNodeWithAutoCreateAndReadLock(
      String path, boolean autoCreateSchema, int sgLevel) throws MetadataException;

  /**
   * get the device node structure, and apply the read lock of the node.
   * If the node does not exist, and IoTDB allows auto create schema, then create it (and its parents)
   * @param path  a path which represents a device.
   * @return the Node that represents to the device. (node.children() will be measurements)
   * @throws MetadataException
   */
  ISchemaNode getDeviceNodeWithAutoCreateAndReadLock(String path) throws MetadataException;

  /**
   * get the device node structure
   * @param path  a path which represents a device.
   * @return
   * @throws MetadataException PathNotExistException or StorageGroupNotSetException
   */
  ISchemaNode getDeviceNode(String path) throws MetadataException;

  /**
   * given a path, find the same string in memory to reduce the memory cost.
   * @param path a path which represents a device.
   * @return
   */
  String getDeviceId(String path);

  /**
   * find the child of a given node.
   * @param parent
   * @param child
   * @return the child node structure or null if not found
   */
  ISchemaNode getChild(ISchemaNode parent, String child);

  /**
   * convert the whole schema into json format (except the first two lines).
   * e.g.,
   * <pre>
   * ===  Timeseries Tree  ===
   *
   * {
   * 	"root":{
   * 		"ln":{
   * 			"wf01":{
   * 				"wt01":{
   * 					"status":{
   * 						"args":"{}",
   * 						"StorageGroup":"root.ln",
   * 						"DataType":"BOOLEAN",
   * 						"Compressor":"UNCOMPRESSED",
   * 						"Encoding":"PLAIN"
   *          }
   *        }
   * 			}
   * 		}
   * 	}
   * }
   * </pre>
   * @return
   */
  String getMetadataInString();

  @TestOnly
  /**
   * just for test.
   * set the maximal number of time series among all storage groups.
   */
  void setMaxSeriesNumberAmongStorageGroup(long maxSeriesNumberAmongStorageGroup);

  /**
   * find the maximal number of time series among all storage groups.
   * e.g., sg1 has 5 series, and sg2 has 10 series, and then the result is 10.
   * @return
   */
  long getMaximalSeriesNumberAmongStorageGroups();

  /**
   * set the data time-to-live under a given storage group.
   * The data will be (lazy) removed after IoTDB instance's system time > dataTTL + data's timestamp
   * @param storageGroup
   * @param dataTTL unit: ms.
   * @throws MetadataException
   * @throws IOException
   */
  void setTTL(String storageGroup, long dataTTL) throws MetadataException, IOException;

  /**
   * get the TTLs of all storage groups
   * @return
   */
  Map<String, Long> getStorageGroupsTTL();

  /**
   * update or insert new alias, tags, and attributes to a given time series
   * @param alias can be null if you do not want to update/insert the alias
   * @param tagsMap can be null if you do not want to update/insert the tags
   * @param attributesMap can be null if you do not want to update/insert the attributes
   * @param fullPath full path of a time series
   * @throws MetadataException
   * @throws IOException
   */
  void upsertTagsAndAttributes(String alias, Map<String, String> tagsMap,
      Map<String, String> attributesMap, String fullPath) throws MetadataException, IOException;

  /**
   * add a set of attribute keys and attribute values to a given series.
   * Attributes does not allow query by attribute value
   * @param attributesMap
   * @param fullPath full path of a time series
   * @throws MetadataException
   * @throws IOException
   */
  void addAttributes(Map<String, String> attributesMap, String fullPath)
          throws MetadataException, IOException;

  /**
   * add a set of tag keys and tag values to a given series.
   * @param tagsMap
   * @param fullPath full path of a time series
   * @throws MetadataException
   * @throws IOException
   */
  void addTags(Map<String, String> tagsMap, String fullPath)
              throws MetadataException, IOException;

  /**
   * remote the given keys from a time series's tags or attributes
   * @param keySet
   * @param fullPath
   * @throws MetadataException
   * @throws IOException
   */
  void dropTagsOrAttributes(Set<String> keySet, String fullPath)
                  throws MetadataException, IOException;

  /**
   * add or change the values of tags or attributes
   *
   * For each key, if the key is an existing tag key, then update its value.
   * If the key is not an existing tag key, then either update the key's value in attributes
   * or save it as an attribute key.
   *
   * @param alterMap the updated tags or new/updated attributes key-value
   * @param fullPath timeseries
   */
  void setTagsOrAttributesValue(Map<String, String> alterMap, String fullPath)
                      throws MetadataException, IOException;
  /**
   * rename the tag or attribute's key of the timeseries
   *
   * @param oldKey   old key of tag or attribute
   * @param newKey   new key of tag or attribute
   * @param fullPath timeseries
   */
  void renameTagOrAttributeKey(String oldKey, String newKey, String fullPath)
                          throws MetadataException, IOException;

  /**
   * collect all timeseries that have the given prefix path of the startingNode
   * @param startingNode
   * @param timeseriesSchemas
   */
  void collectTimeseriesSchema(ISchemaNode startingNode,
      Collection<TimeseriesSchema> timeseriesSchemas);

  /**
   * collect all timeseries that have the given prefix path of the startingNode
   * @param startingNode
   * @param timeseriesSchemas
   */
  void collectMeasurementSchema(ISchemaNode startingNode,
      Collection<MeasurementSchema> timeseriesSchemas);

  /**
   * the same to collectMeasurementSchema(ISchemaNode startingNode,
   *       Collection<MeasurementSchema> timeseriesSchemas)
   * @param startingPath
   * @param measurementSchemas
   */
  void collectSeries(String startingPath, List<MeasurementSchema> measurementSchemas);

  /**
   * For a path, infer all storage groups it may belong to.
   *
   * The input path can have *. Therefore, there may be more than one storage groups match the prefix
   * of the path.
   *
   * given a path, find all storage groups that can match the prefix of the path.
   * For each storage group, use the storage group to replace the prefix of the path if the prefix contains *.
   * Collect all storage groups as keys and the replaced paths as values.
   *
   * Notice:
   *
   * If the * is not at the tail of the path, then only one level will be inferred by the *.
   * If the wildcard is at the tail, then the inference will go on until the storage groups are found
   * and the wildcard will be kept.
   *
   * <p>
   *   Assuming we have three SGs: root.group1, root.group2, root.area1.group3
   *   <br/>Eg1: for input "root.*",
   *   returns ("root.group1", "root.group1.*"), ("root.group2", "root.group2.*")
   * ("root.area1.group3", "root.area1.group3.*")
   * <br/> Eg2: for input "root.*.s1.*.b",
   * returns ("root.group1", "root.group1.s1.*.b"), ("root.group2", "root.group2.s1.*.b")
   *
   * <p>Eg3: for input "root.area1.*", returns ("root.area1.group3", "root.area1.group3.*")
   *
   * @param path can be a prefix or a full path. can has *.
   * @return StorageGroupName-FullPath pairs
   */
  Map<String, String> determineStorageGroup(String path) throws IllegalPathException;

  /**
   * cache a given time series schema
   * @param path
   * @param meta
   */
  void cacheMeta(String path, MeasurementMeta meta);

  /**
   *
   * @param seriesPath
   * @param timeValuePair
   * @param highPriorityUpdate
   * @param latestFlushedTime
   * @param node
   */
  void updateLastCache(String seriesPath, TimeValuePair timeValuePair,
      boolean highPriorityUpdate, Long latestFlushedTime,
      IMeasurementMNode node);

  /**
   * get the lastest data point of the given time series
   * @param seriesPath a full path
   * @return
   */
  TimeValuePair getLastCache(String seriesPath);

  /**
   * Create a schema snapshot.
   * This is just for accerelating the speed of restarting IoTDB.
   * If the init() is very fast, this method can be ignored.
   */
  void createMTreeSnapshot();

  /**
   * 1. If the related time series are not registered, and the IoTDB instance allows auto-create-schema,
   * then create related time series.
   * 2. For those existing time series, check whether the data types in the insertPlan are correct.
   * 2.1 if IoTDB enable partial insert, then call plan.markFailedMeasurementInsertion() to record
   * which measurements have incorrect data type. Else throw MetadataException if there is error.
   * 3. Getting the DeviceNode according to the deviceID, and assign it to the insertPlan
   * 4. Apply for the readlock of the deviceNode.
   * @param deviceId the device path, must be the same in the insertPlan
   * @param measurementList the measurement names, must be the same in the isnertPlan
   * @param plan
   * @return the measurement schema of each measurement.
   * @throws MetadataException
   */
  MeasurementSchema[] getSeriesSchemasAndReadLockDevice(String deviceId,
      String[] measurementList, InsertPlan plan) throws MetadataException;

  /**
   * unlock the read lock of the given deviceId.
   * @param deviceId
   */
  void unlockDeviceReadLock(String deviceId);
}
