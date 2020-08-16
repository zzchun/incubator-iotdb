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
package org.apache.iotdb.db.engine;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.ServerConfigConsistent;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.*;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryFileManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class StorageEngine implements IService {

  private final Logger logger;
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long TTL_CHECK_INTERVAL = 60 * 1000L;

  /**
   * a folder (system/storage_groups/ by default) that persist system info. Each Storage Processor
   * will have a subfolder under the systemDir.
   */
  private final String systemDir;

  /**
   * storage group name -> storage group processor
   */
  private final ConcurrentHashMap<PartialPath, StorageGroupProcessor> processorMap = new ConcurrentHashMap<>();

  private static final ExecutorService recoveryThreadPool = IoTDBThreadPoolFactory
      .newFixedThreadPool(Runtime.getRuntime().availableProcessors(), "Recovery-Thread-Pool");

  public boolean isAllSgReady() {
    return isAllSgReady.get();
  }

  public void setAllSgReady(boolean allSgReady) {
    isAllSgReady.set(allSgReady);
  }

  private AtomicBoolean isAllSgReady = new AtomicBoolean(false);
  private ExecutorService recoverAllSgThreadPool;

  static class InstanceHolder {

    private InstanceHolder() {
      // forbidding instantiation
    }

    private static final StorageEngine INSTANCE = new StorageEngine();
  }

  public static StorageEngine getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private ScheduledExecutorService ttlCheckThread;
  private TsFileFlushPolicy fileFlushPolicy = new DirectFlushPolicy();

  /**
   * Time range for dividing storage group, the time unit is the same with IoTDB's
   * TimestampPrecision
   */
  @ServerConfigConsistent
  private static long timePartitionInterval = -1;

  /**
   * whether enable data partition
   * if disabled, all data belongs to partition 0
   */
  @ServerConfigConsistent
  private static boolean enablePartition =
      IoTDBDescriptor.getInstance().getConfig().isEnablePartition();

  private StorageEngine() {
    logger = LoggerFactory.getLogger(StorageEngine.class);
    systemDir = FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";

    // build time Interval to divide time partition
    if (!enablePartition) {
      timePartitionInterval = Long.MAX_VALUE;
    } else {
      initTimePartition();
    }

    // create systemDir
    try {
      FileUtils.forceMkdir(SystemFileFactory.INSTANCE.getFile(systemDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }
    // recover upgrade process
    UpgradeUtils.recoverUpgrade();

    recover();
  }

  public void recover() {
    recoverAllSgThreadPool = IoTDBThreadPoolFactory
      .newSingleThreadExecutor("Begin-Recovery-Pool");
    recoverAllSgThreadPool.submit(this::recoverAllSgs);
  }

  private void recoverAllSgs() {
    /*
     * recover all storage group processors.
     */
    List<StorageGroupMNode> sgNodes = IoTDB.metaManager.getAllStorageGroupNodes();
    List<Future> futures = new ArrayList<>();
    for (StorageGroupMNode storageGroup : sgNodes) {
      futures.add(recoveryThreadPool.submit((Callable<Void>) () -> {
        try {
          StorageGroupProcessor processor = new StorageGroupProcessor(systemDir,
            storageGroup.getFullPath(), fileFlushPolicy);
          processor.setDataTTL(storageGroup.getDataTTL());
          processorMap.put(storageGroup.getPartialPath(), processor);
          logger.info("Storage Group Processor {} is recovered successfully",
            storageGroup.getFullPath());
        } catch (Exception e) {
          logger.error("meet error when recovering storage group: {}", storageGroup.getFullPath(), e);
        }
        return null;
      }));
    }
    for (Future future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
      }
    }
    recoveryThreadPool.shutdown();
    setAllSgReady(true);
  }

  private static void initTimePartition() {
    timePartitionInterval = convertMilliWithPrecision(IoTDBDescriptor.getInstance().
        getConfig().getPartitionInterval() * 1000L);
  }

  public static long convertMilliWithPrecision(long milliTime) {
    long result = milliTime;
    String timePrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ns":
        result = milliTime * 1000_000L;
        break;
      case "us":
        result = milliTime * 1000L;
        break;
      default:
        break;
    }
    return result;
  }

  @Override
  public void start() {
    ttlCheckThread = Executors.newSingleThreadScheduledExecutor();
    ttlCheckThread.scheduleAtFixedRate(this::checkTTL, TTL_CHECK_INTERVAL, TTL_CHECK_INTERVAL
        , TimeUnit.MILLISECONDS);
  }

  private void checkTTL() {
    try {
      for (StorageGroupProcessor processor : processorMap.values()) {
        processor.checkFilesTTL();
      }
    } catch (ConcurrentModificationException e) {
      // ignore
    } catch (Exception e) {
      logger.error("An error occurred when checking TTL", e);
    }

    if (isAllSgReady.get() && !recoverAllSgThreadPool.isShutdown()) {
      recoverAllSgThreadPool.shutdownNow();
    }
  }

  @Override
  public void stop() {
    syncCloseAllProcessor();
    if (ttlCheckThread != null) {
      ttlCheckThread.shutdownNow();
      try {
        ttlCheckThread.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("TTL check thread still doesn't exit after 30s");
      }
    }
    recoveryThreadPool.shutdownNow();
    if (!recoverAllSgThreadPool.isShutdown()) {
      recoverAllSgThreadPool.shutdownNow();
    }
    this.reset();
  }

  @Override
  public void shutdown(long millseconds) throws ShutdownException {
    try {
      forceCloseAllProcessor();
    } catch (TsFileProcessorException e) {
      throw new ShutdownException(e);
    }
    if (ttlCheckThread != null) {
      ttlCheckThread.shutdownNow();
      try {
        ttlCheckThread.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("TTL check thread still doesn't exit after 30s");
        Thread.currentThread().interrupt();
      }
    }
    recoveryThreadPool.shutdownNow();
    this.reset();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }

  public StorageGroupProcessor getProcessor(PartialPath path) throws StorageEngineException {
    PartialPath storageGroup;
    try {
      storageGroup = IoTDB.metaManager.getStorageGroupName(path);
      StorageGroupProcessor processor;
      processor = processorMap.get(storageGroup);
      if (processor == null) {
        // if finish recover
        if (isAllSgReady.get()) {
          synchronized (storageGroup) {
            processor = processorMap.get(storageGroup);
            if (processor == null) {
              logger.info("construct a processor instance, the storage group is {}, Thread is {}",
                storageGroup, Thread.currentThread().getId());
              processor = new StorageGroupProcessor(systemDir, storageGroup.toString(), fileFlushPolicy);
              StorageGroupMNode storageGroupMNode = IoTDB.metaManager
                .getStorageGroupNode(storageGroup);
              processor.setDataTTL(storageGroupMNode.getDataTTL());
              processorMap.put(storageGroup, processor);
            }
          }
        } else {
          // not finished recover, refuse the request
          throw new StorageEngineException("the sg " + storageGroup + " may not ready now, please wait and retry later",
              TSStatusCode.STORAGE_GROUP_NOT_READY.getStatusCode());
        }
      }
      return processor;
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }


  /**
   * This function is just for unit test.
   */
  public synchronized void reset() {
    processorMap.clear();
  }


  /**
   * insert an InsertRowPlan to a storage group.
   *
   * @param insertRowPlan physical plan of insertion
   */
  public void insert(InsertRowPlan insertRowPlan) throws StorageEngineException {

    StorageGroupProcessor storageGroupProcessor = getProcessor(insertRowPlan.getDeviceId());

    // TODO monitor: update statistics
    try {
      storageGroupProcessor.insert(insertRowPlan);
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * insert a InsertTabletPlan to a storage group
   */
  public void insertTablet(InsertTabletPlan insertTabletPlan)
      throws StorageEngineException, BatchInsertionException {
    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(insertTabletPlan.getDeviceId());
    } catch (StorageEngineException e) {
      throw new StorageEngineException(String.format("Get StorageGroupProcessor of device %s "
          + "failed", insertTabletPlan.getDeviceId()), e);
    }

    // TODO monitor: update statistics
    storageGroupProcessor.insertTablet(insertTabletPlan);
  }

  /**
   * flush command Sync asyncCloseOneProcessor all file node processors.
   */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all storage group processor");
    for (StorageGroupProcessor processor : processorMap.values()) {
      processor.syncCloseAllWorkingTsFileProcessors();
    }
  }

  public void forceCloseAllProcessor() throws TsFileProcessorException {
    logger.info("Start closing all storage group processor");
    for (StorageGroupProcessor processor : processorMap.values()) {
      processor.forceCloseAllWorkingTsFileProcessors();
    }
  }

  public void asyncCloseProcessor(String storageGroupName, boolean isSeq)
      throws StorageGroupNotSetException {
    StorageGroupProcessor processor = processorMap.get(storageGroupName);
    if (processor != null) {
      logger.info("async closing sg processor is called for closing {}, seq = {}", storageGroupName,
          isSeq);
      processor.writeLock();
      try {
        if (isSeq) {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor : new ArrayList<>(
              processor.getWorkSequenceTsFileProcessors())) {
            processor.asyncCloseOneTsFileProcessor(true, tsfileProcessor);
          }
        } else {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor : new ArrayList<>(
              processor.getWorkUnsequenceTsFileProcessor())) {
            processor.asyncCloseOneTsFileProcessor(false, tsfileProcessor);
          }
        }
      } finally {
        processor.writeUnlock();
      }
    } else {
      throw new StorageGroupNotSetException(storageGroupName);
    }
  }

  public void asyncCloseProcessor(String storageGroupName, long partitionId, boolean isSeq)
      throws StorageGroupNotSetException {
    StorageGroupProcessor processor = processorMap.get(storageGroupName);
    if (processor != null) {
      logger.info("async closing sg processor is called for closing {}, seq = {}, partitionId = {}",
          storageGroupName, isSeq, partitionId);
      processor.writeLock();
      // to avoid concurrent modification problem, we need a new array list
      List<TsFileProcessor> processors = isSeq ?
          new ArrayList<>(processor.getWorkSequenceTsFileProcessors()) :
          new ArrayList<>(processor.getWorkUnsequenceTsFileProcessor());
      try {
        for (TsFileProcessor tsfileProcessor : processors) {
          if (tsfileProcessor.getTimeRangeId() == partitionId) {
            processor.asyncCloseOneTsFileProcessor(isSeq, tsfileProcessor);
            break;
          }
        }
      } finally {
        processor.writeUnlock();
      }
    } else {
      throw new StorageGroupNotSetException(storageGroupName);
    }
  }

  /**
   * update data.
   */
  public void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, String v) {
    // TODO
  }

  /**
   * delete data of timeseries "{deviceId}.{measurementId}" with time <= timestamp.
   */
  public void delete(PartialPath deviceId, String measurementId, long startTime, long endTime)
      throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    try {
      storageGroupProcessor.delete(deviceId, measurementId, startTime, endTime);
    } catch (IOException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /**
   * query data.
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context,
      QueryFileManager filePathsManager)
      throws StorageEngineException, QueryProcessException, IllegalPathException {
    PartialPath deviceId = new PartialPath(seriesExpression.getSeriesPath().getDevice());
    String measurementId = seriesExpression.getSeriesPath().getMeasurement();
    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    return storageGroupProcessor
        .query(deviceId, measurementId, context, filePathsManager, seriesExpression.getFilter());
  }

  /**
   * count all Tsfiles which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded
   */
  public int countUpgradeFiles() {
    int totalUpgradeFileNum = 0;
    for (StorageGroupProcessor storageGroupProcessor : processorMap.values()) {
      totalUpgradeFileNum += storageGroupProcessor.countUpgradeFiles();
    }
    return totalUpgradeFileNum;
  }

  /**
   * upgrade all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void upgradeAll() throws StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException(
          "Current system mode is read only, does not support file upgrade");
    }
    for (StorageGroupProcessor storageGroupProcessor : processorMap.values()) {
      storageGroupProcessor.upgrade();
    }
  }

  /**
   * merge all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void mergeAll(boolean fullMerge) throws StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException("Current system mode is read only, does not support merge");
    }
    for (StorageGroupProcessor storageGroupProcessor : processorMap.values()) {
      storageGroupProcessor.merge(fullMerge);
    }
  }

  /**
   * delete all data files (both memory data and file on disk) in a storage group. It is used when
   * there is no timeseries (which are all deleted) in this storage group)
   */
  public void deleteAllDataFilesInOneStorageGroup(PartialPath storageGroupName) {
    if (processorMap.containsKey(storageGroupName)) {
      syncDeleteDataFiles(storageGroupName);
    }
  }

  private void syncDeleteDataFiles(PartialPath storageGroupName) {
    logger.info("Force to delete the data in storage group processor {}", storageGroupName);
    StorageGroupProcessor processor = processorMap.get(storageGroupName);
    processor.syncDeleteDataFiles();
  }

  /**
   * delete all data of storage groups' timeseries.
   */
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    syncCloseAllProcessor();
    for (PartialPath storageGroup : IoTDB.metaManager.getAllStorageGroupNames()) {
      this.deleteAllDataFilesInOneStorageGroup(storageGroup);
    }
    return true;
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor = getProcessor(storageGroup);
    storageGroupProcessor.setDataTTL(dataTTL);
  }

  public void deleteStorageGroup(PartialPath storageGroupName) {
    deleteAllDataFilesInOneStorageGroup(storageGroupName);
    StorageGroupProcessor processor = processorMap.remove(storageGroupName);
    if (processor != null) {
      processor.deleteFolder(systemDir);
    }
  }

  public void loadNewTsFileForSync(TsFileResource newTsFileResource)
      throws StorageEngineException, LoadFileException, IllegalPathException {
    getProcessor(new PartialPath(newTsFileResource.getTsFile().getParentFile().getName()))
        .loadNewTsFileForSync(newTsFileResource);
  }

  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws LoadFileException, StorageEngineException, MetadataException {
    Map<String, Integer> deviceMap = newTsFileResource.getDeviceToIndexMap();
    if (deviceMap == null || deviceMap.isEmpty()) {
      throw new StorageEngineException("Can not get the corresponding storage group.");
    }
    String device = deviceMap.keySet().iterator().next();
    PartialPath devicePath = new PartialPath(device);
    PartialPath storageGroupName = IoTDB.metaManager.getStorageGroupName(devicePath);
    getProcessor(storageGroupName).loadNewTsFile(newTsFileResource);
  }

  public boolean deleteTsfileForSync(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessor(new PartialPath(deletedTsfile.getParentFile().getName())).deleteTsfile(deletedTsfile);
  }

  public boolean deleteTsfile(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessor(new PartialPath(getSgByEngineFile(deletedTsfile))).deleteTsfile(deletedTsfile);
  }

  public boolean moveTsfile(File tsfileToBeMoved, File targetDir)
      throws StorageEngineException, IOException, IllegalPathException {
    return getProcessor(new PartialPath(getSgByEngineFile(tsfileToBeMoved))).moveTsfile(tsfileToBeMoved, targetDir);
  }

  /**
   * The internal file means that the file is in the engine, which is different from those external
   * files which are not loaded.
   *
   * @param file internal file
   * @return sg name
   */
  private String getSgByEngineFile(File file) {
    return file.getParentFile().getParentFile().getName();
  }

  /**
   * @return TsFiles (seq or unseq) grouped by their storage group and partition number.
   */
  public Map<PartialPath, Map<Long, List<TsFileResource>>> getAllClosedStorageGroupTsFile() {
    Map<PartialPath, Map<Long, List<TsFileResource>>> ret = new HashMap<>();
    for (Entry<PartialPath, StorageGroupProcessor> entry : processorMap.entrySet()) {
      List<TsFileResource> allResources = entry.getValue().getSequenceFileTreeSet();
      allResources.addAll(entry.getValue().getUnSequenceFileList());
      for (TsFileResource sequenceFile : allResources) {
        if (!sequenceFile.isClosed()) {
          continue;
        }
        long partitionNum = sequenceFile.getTimePartition();
        Map<Long, List<TsFileResource>> storageGroupFiles = ret.computeIfAbsent(entry.getKey()
            , n -> new HashMap<>());
        storageGroupFiles.computeIfAbsent(partitionNum, n -> new ArrayList<>()).add(sequenceFile);
      }
    }
    return ret;
  }

  public void setFileFlushPolicy(TsFileFlushPolicy fileFlushPolicy) {
    this.fileFlushPolicy = fileFlushPolicy;
  }

  public boolean isFileAlreadyExist(TsFileResource tsFileResource, String storageGroup,
      long partitionNum) {
    StorageGroupProcessor processor = processorMap.get(storageGroup);
    return processor != null && processor.isFileAlreadyExist(tsFileResource, partitionNum);
  }

  public static long getTimePartitionInterval() {
    if (timePartitionInterval == -1) {
      initTimePartition();
    }
    return timePartitionInterval;
  }

  @TestOnly
  public static void setTimePartitionInterval(long timePartitionInterval) {
    StorageEngine.timePartitionInterval = timePartitionInterval;
  }

  public static long getTimePartition(long time) {
    return enablePartition ? time / timePartitionInterval : 0;
  }

  /**
   * Set the version of given partition to newMaxVersion if it is larger than the current version.
   * @param storageGroup
   * @param partitionId
   * @param newMaxVersion
   */
  public void setPartitionVersionToMax(PartialPath storageGroup, long partitionId, long newMaxVersion)
      throws StorageEngineException {
    getProcessor(storageGroup).setPartitionFileVersionToMax(partitionId, newMaxVersion);
  }


  public void removePartitions(PartialPath storageGroupName, TimePartitionFilter filter)
      throws StorageEngineException {
    getProcessor(storageGroupName).removePartitions(filter);
  }

  @TestOnly
  public static void setEnablePartition(boolean enablePartition) {
    StorageEngine.enablePartition = enablePartition;
  }
}
