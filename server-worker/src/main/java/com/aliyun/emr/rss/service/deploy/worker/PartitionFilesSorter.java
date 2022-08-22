/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.rss.service.deploy.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.metrics.source.AbstractSource;
import com.aliyun.emr.rss.common.network.server.FileInfo;
import com.aliyun.emr.rss.common.network.server.MemoryTracker;
import com.aliyun.emr.rss.common.unsafe.Platform;
import com.aliyun.emr.rss.common.util.PBSerDeUtils;
import com.aliyun.emr.rss.common.util.ThreadUtils;

public class PartitionFilesSorter extends WorkerRecoverHelper {
  private static final Logger logger = LoggerFactory.getLogger(PartitionFilesSorter.class);
  public static final String SORTED_SUFFIX = ".sorted";
  public static final String INDEX_SUFFIX = ".index";
  private LevelDBProvider.StoreVersion CURRENT_VERSION = new LevelDBProvider.StoreVersion(1, 0);
  private String RECOVERY_SORTED_FILES_FILE_NAME = "sortedFiles.ldb";
  private File recoverFile;
  private volatile boolean shutdown = false;
  private final ConcurrentHashMap<String, Set<String>> sortedShuffleFiles =
    new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<String>> sortingShuffleFiles =
    new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Map<String, Map<Integer, List<ShuffleBlockInfo>>>>
    cachedIndexMaps = new ConcurrentHashMap<>();
  private final LinkedBlockingQueue<FileSorter> shuffleSortTaskDeque = new LinkedBlockingQueue<>();
  protected final long sortTimeout;
  protected final long fetchChunkSize;
  protected final long reserveMemoryForSingleSort;
  private boolean gracefulShutdown = false;
  private long partitionSorterShutdownAwaitTime;
  private DB sortedFilesDb;

  protected final AbstractSource source;

  private final ExecutorService fileSorterExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "worker-file-sorter-execute", Math.max(Runtime.getRuntime().availableProcessors(), 8), 120);
  private final Thread fileSorterSchedulerThread;

  PartitionFilesSorter(MemoryTracker memoryTracker, RssConf conf, AbstractSource source) {
    this.sortTimeout = RssConf.partitionSortTimeout(conf);
    this.fetchChunkSize = RssConf.workerFetchChunkSize(conf);
    this.reserveMemoryForSingleSort =  RssConf.memoryReservedForSingleSort(conf);
    this.partitionSorterShutdownAwaitTime = RssConf.partitionSorterCloseAwaitTimeMs(conf);
    this.source = source;
    this.gracefulShutdown = RssConf.workerGracefulShutdown(conf);
    // ShuffleClient can fetch shuffle data from a restarted worker only
    // when the worker's fetching port is stable and enables graceful shutdown.
    if (gracefulShutdown) {
      try {
        String recoverPath = RssConf.workerRecoverPath(conf);
        this.recoverFile = new File(recoverPath, RECOVERY_SORTED_FILES_FILE_NAME);
        this.sortedFilesDb = LevelDBProvider.initLevelDB(recoverFile, CURRENT_VERSION);
        reloadAndCleanSortedShuffleFiles(this.sortedFilesDb);
      } catch (Exception e) {
        logger.error("Failed to reload LevelDB for sorted shuffle files from: " + recoverFile, e);
        this.sortedFilesDb = null;
      }
    } else {
      this.sortedFilesDb = null;
    }

    fileSorterSchedulerThread = new Thread(() -> {
      try {
        while (!shutdown) {
          FileSorter task = shuffleSortTaskDeque.take();
          memoryTracker.reserveSortMemory(reserveMemoryForSingleSort);
          while (!memoryTracker.sortMemoryReady()) {
            Thread.sleep(20);
          }
          fileSorterExecutors.submit(() -> {
            source.startTimer(WorkerSource.SortTime(), task.fileId);
            task.sort();
            source.stopTimer(WorkerSource.SortTime(), task.fileId);
            memoryTracker.releaseSortMemory(reserveMemoryForSingleSort);
          });
        }
      } catch (InterruptedException e) {
        logger.warn("Sort thread is shutting down, detail :", e);
      }
    });
    fileSorterSchedulerThread.start();
  }

  public int getSortingCount() {
    return shuffleSortTaskDeque.size();
  }

  public FileInfo openStream(String shuffleKey, String fileName, FileInfo fileInfo,
    int startMapIndex, int endMapIndex) {
    if (endMapIndex == Integer.MAX_VALUE) {
      return fileInfo;
    } else {
      String fileId = shuffleKey + "-" + fileName;

      Set<String> sorted =
        sortedShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());
      Set<String> sorting =
        sortingShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());

      String sortedFileName = fileInfo.getFile().getAbsolutePath() + SORTED_SUFFIX;
      String indexFileName = fileInfo.getFile().getAbsolutePath() + INDEX_SUFFIX;

      if (sorted.contains(fileId)) {
        return resolve(shuffleKey, fileId, sortedFileName, indexFileName,
          startMapIndex, endMapIndex);
      }

      synchronized (sorting) {
        if (!sorting.contains(fileId)) {
          FileSorter fileSorter = new FileSorter(fileInfo.getFile(), fileInfo.getFileLength(),
            fileId, shuffleKey);
          sorting.add(fileId);
          try {
            shuffleSortTaskDeque.put(fileSorter);
          } catch (InterruptedException e) {
            logger.info("scheduler thread is interrupted means worker is shutting down.");
            return null;
          }
        }
      }

      long sortStartTime = System.currentTimeMillis();
      while (!sorted.contains(fileId)) {
        if (sorting.contains(fileId)) {
          try {
            Thread.sleep(50);
            if (System.currentTimeMillis() - sortStartTime > sortTimeout) {
              logger.error("sort file {} timeout", fileId);
              return null;
            }
          } catch (InterruptedException e) {
            logger.error("sort scheduler thread is interrupted means worker is shutting down.", e);
            return null;
          }
        } else {
          logger.error("file {} sort failed", fileId);
          return null;
        }
      }

      return resolve(shuffleKey, fileId, sortedFileName, indexFileName,
        startMapIndex, endMapIndex);
    }
  }

  public void cleanup(HashSet<String> expiredShuffleKeys) {
    for (String expiredShuffleKey : expiredShuffleKeys) {
      sortingShuffleFiles.remove(expiredShuffleKey);
      deleteSortedShuffleFiles(expiredShuffleKey);
      cachedIndexMaps.remove(expiredShuffleKey);
    }
  }

  public void close() {
    logger.info("Start close " + this.getClass().getSimpleName());
    shutdown = true;
    if (gracefulShutdown) {
      long start = System.currentTimeMillis();
      try {
        fileSorterExecutors.shutdown();
        fileSorterExecutors.awaitTermination(
            partitionSorterShutdownAwaitTime, TimeUnit.MILLISECONDS);
        if (!fileSorterExecutors.isShutdown()) {
          fileSorterExecutors.shutdownNow();
        }
      } catch (InterruptedException e) {
        logger.error("Await partition sorter executor shutdown catch exception: ", e);
      }
      long end = System.currentTimeMillis();
      logger.info("Await partition sorter executor complete cost " + (end - start) + "ms");
    } else {
      fileSorterSchedulerThread.interrupt();
      fileSorterExecutors.shutdownNow();
    }
    cachedIndexMaps.clear();
    if (sortedFilesDb != null) {
      try {
        updateSortedShuffleFilesInDB();
        sortedFilesDb.close();
      } catch (IOException e) {
        logger.error("Store recover data to LevelDB failed.", e);
      }
    }
  }

  private void reloadAndCleanSortedShuffleFiles(DB db) {
    if (db != null) {
      DBIterator itr = db.iterator();
      itr.seek(SHUFFLE_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
      while (itr.hasNext()) {
        Map.Entry<byte[], byte[]> entry = itr.next();
        String key = new String(entry.getKey(), StandardCharsets.UTF_8);
        if (key.startsWith(SHUFFLE_KEY_PREFIX)) {
          String shuffleKey = parseDbShuffleKey(key);
          try {
            Set<String> sortedFiles = PBSerDeUtils.fromPbSortedShuffleFileSet(entry.getValue());
            logger.debug("Reload DB: " + shuffleKey + " -> " + sortedFiles);
            sortedShuffleFiles.put(shuffleKey, sortedFiles);
            sortedFilesDb.delete(entry.getKey());
          } catch (Exception exception) {
            logger.error("Reload DB: " + shuffleKey + " failed.", exception);
          }
        }
      }
    }
  }

  @VisibleForTesting
  public void updateSortedShuffleFilesInDB() {
    for (String shuffleKey : sortedShuffleFiles.keySet()) {
      try {
        sortedFilesDb.put(dbShuffleKey(shuffleKey),
            PBSerDeUtils.toPbSortedShuffleFileSet(sortedShuffleFiles.get(shuffleKey)));
        logger.debug("Update DB: " + shuffleKey + " -> " + sortedShuffleFiles.get(shuffleKey));
      } catch (Exception exception) {
        logger.error("Update DB: " + shuffleKey + " failed.", exception);
      }
    }
  }

  @VisibleForTesting
  public Set<String> initSortedShuffleFiles(String shuffleKey) {
    return sortedShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());
  }

  @VisibleForTesting
  public void updateSortedShuffleFiles(String shuffleKey, String fileId) {
    sortedShuffleFiles.get(shuffleKey).add(fileId);
  }

  @VisibleForTesting
  public void deleteSortedShuffleFiles(String expiredShuffleKey) {
    sortedShuffleFiles.remove(expiredShuffleKey);
  }

  @VisibleForTesting
  public Set<String> getSortedShuffleFiles(String shuffleKey) {
    return sortedShuffleFiles.get(shuffleKey);
  }

  protected void writeIndex(Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFileName)
    throws IOException {
    File indexFile = new File(indexFileName);
    FileChannel indexFileChannel = new FileOutputStream(indexFile).getChannel();

    int indexSize = 0;
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
      indexSize += 8;
      indexSize += entry.getValue().size() * 16;
    }

    ByteBuffer indexBuf = ByteBuffer.allocateDirect(indexSize);
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
      int mapId = entry.getKey();
      List<ShuffleBlockInfo> list = entry.getValue();
      indexBuf.putInt(mapId);
      indexBuf.putInt(list.size());
      list.forEach(info -> {
        indexBuf.putLong(info.offset);
        indexBuf.putLong(info.length);
      });
    }

    indexBuf.flip();
    indexFileChannel.write(indexBuf);
    indexFileChannel.close();
    ((DirectBuffer) indexBuf).cleaner().clean();
  }

  protected Map<Integer, List<ShuffleBlockInfo>> readIndex(ByteBuffer indexBuf) {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();
    while (indexBuf.hasRemaining()) {
      int mapId = indexBuf.getInt();
      int count = indexBuf.getInt();
      List<ShuffleBlockInfo> blockInfos = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        long offset = indexBuf.getLong();
        long length = indexBuf.getLong();
        ShuffleBlockInfo info = new ShuffleBlockInfo();
        info.offset = offset;
        info.length = length;
        blockInfos.add(info);
      }
      indexMap.put(mapId, blockInfos);
    }
    return indexMap;
  }

  protected void readFully(FileChannel channel, ByteBuffer buffer, String filePath)
    throws IOException {
    while (buffer.hasRemaining()) {
      if (-1 == channel.read(buffer)) {
        throw new IOException("Unexpected EOF, file name : " + filePath +
          " position :" + channel.position() + " buffer size :" + buffer.limit());
      }
    }
  }

  private long transferFully(FileChannel originChannel, FileChannel targetChannel,
    long offset, long length) throws IOException {
    long transferedSize = 0;
    while (transferedSize != length) {
      transferedSize += originChannel.transferTo(offset + transferedSize,
        length - transferedSize, targetChannel);
    }
    return transferedSize;
  }

  ArrayList<Long> getChunkOffsets(int startMapIndex, int endMapIndex, String sortedFileName,
    Map<Integer, List<ShuffleBlockInfo>> indexMap) {
    ArrayList<Long> sortedChunkOffset = new ArrayList<>();
    ShuffleBlockInfo lastBlock = null;
    logger.debug("Refresh offsets for file {} , startMapIndex {} endMapIndex {}",
      sortedFileName, startMapIndex, endMapIndex);
    for (int i = startMapIndex; i < endMapIndex; i++) {
      List<ShuffleBlockInfo> blockInfos = indexMap.get(i);
      if (blockInfos != null) {
        for (ShuffleBlockInfo info : blockInfos) {
          if (sortedChunkOffset.size() == 0) {
            sortedChunkOffset.add(info.offset);
          }
          if (info.offset - sortedChunkOffset.get(sortedChunkOffset.size() - 1) > fetchChunkSize) {
            sortedChunkOffset.add(info.offset);
          }
          lastBlock = info;
        }
      }
    }
    if (lastBlock != null) {
      long endChunkOffset = lastBlock.length + lastBlock.offset;
      if (!sortedChunkOffset.contains(endChunkOffset)) {
        sortedChunkOffset.add(endChunkOffset);
      }
    }
    return sortedChunkOffset;
  }

  public FileInfo resolve(String shuffleKey, String fileId, String sortedFileName,
    String indexFileName, int startMapIndex, int endMapIndex) {
    Map<Integer, List<ShuffleBlockInfo>> indexMap;
    if (cachedIndexMaps.containsKey(shuffleKey) &&
          cachedIndexMaps.get(shuffleKey).containsKey(fileId)) {
      indexMap = cachedIndexMaps.get(shuffleKey).get(fileId);
    } else {
      try (FileInputStream indexStream = new FileInputStream(indexFileName)) {
        File indexFile = new File(indexFileName);
        int indexSize = (int) indexFile.length();
        ByteBuffer indexBuf = ByteBuffer.allocateDirect(indexSize);
        readFully(indexStream.getChannel(), indexBuf, indexFileName);
        indexBuf.rewind();
        indexMap = readIndex(indexBuf);
        ((DirectBuffer) indexBuf).cleaner().clean();
        Map<String, Map<Integer, List<ShuffleBlockInfo>>> cacheMap =
          cachedIndexMaps.computeIfAbsent(shuffleKey, v -> new ConcurrentHashMap<>());
        cacheMap.put(fileId, indexMap);
      } catch (Exception e) {
        logger.error("Read sorted shuffle file error, detail : ", e);
        return null;
      }
    }
    return new FileInfo(new File(sortedFileName),
      getChunkOffsets(startMapIndex, endMapIndex, sortedFileName, indexMap));
  }

  static class ShuffleBlockInfo {
    protected long offset;
    protected long length;
  }

  class FileSorter {
    private final File originFile;
    private final String sortedFileName;
    private final String indexFileName;
    private final long originFileLen;
    private final String fileId;
    private final String shuffleKey;

    FileSorter(File originFile, long originFileLen, String fileId, String shuffleKey) {
      this.originFile = originFile;
      this.sortedFileName = originFile.getAbsolutePath() + SORTED_SUFFIX;
      this.indexFileName = originFile.getAbsolutePath() + INDEX_SUFFIX;
      this.originFileLen = originFileLen;
      this.fileId = fileId;
      this.shuffleKey = shuffleKey;
    }

    public void sort() {
      try (FileChannel originFileChannel = new FileInputStream(originFile).getChannel();
           FileChannel sortedFileChannel = new FileOutputStream(sortedFileName).getChannel()) {
        int batchHeaderLen = 16;

        Map<Integer, List<ShuffleBlockInfo>> originShuffleBlockInfos = new TreeMap<>();
        Map<Integer, List<ShuffleBlockInfo>> sortedBlockInfoMap = new HashMap<>();

        ByteBuffer headerBuf = ByteBuffer.allocate(batchHeaderLen);
        ByteBuffer paddingBuf = ByteBuffer.allocateDirect((int) reserveMemoryForSingleSort);

        long index = 0;
        while (index != originFileLen) {
          final long blockStartIndex = index;
          readFully(originFileChannel, headerBuf, originFile.getAbsolutePath());
          byte[] batchHeader = headerBuf.array();
          headerBuf.rewind();

          int mapId = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET);
          final int compressedSize = Platform.getInt(batchHeader,
            Platform.BYTE_ARRAY_OFFSET + 12);

          List<ShuffleBlockInfo> singleMapIdShuffleBlockList =
            originShuffleBlockInfos.computeIfAbsent(mapId,
              v -> new ArrayList<>());
          ShuffleBlockInfo blockInfo = new ShuffleBlockInfo();
          blockInfo.offset = blockStartIndex;
          blockInfo.length = compressedSize + 16;
          singleMapIdShuffleBlockList.add(blockInfo);

          index += batchHeaderLen + compressedSize;
          paddingBuf.clear();
          paddingBuf.limit(compressedSize);
          readFully(originFileChannel, paddingBuf, originFile.getAbsolutePath());
        }

        long fileIndex = 0;
        for (Map.Entry<Integer, List<ShuffleBlockInfo>>
               originBlockInfoEntry : originShuffleBlockInfos.entrySet()) {
          int mapId = originBlockInfoEntry.getKey();
          List<ShuffleBlockInfo> originShuffleBlocks = originBlockInfoEntry.getValue();
          List<ShuffleBlockInfo> sortedShuffleBlocks = new ArrayList<>();
          for (ShuffleBlockInfo blockInfo : originShuffleBlocks) {
            long offset = blockInfo.offset;
            long length = blockInfo.length;
            ShuffleBlockInfo sortedBlock = new ShuffleBlockInfo();
            sortedBlock.offset = fileIndex;
            sortedBlock.length = length;
            sortedShuffleBlocks.add(sortedBlock);
            fileIndex += transferFully(originFileChannel, sortedFileChannel, offset, length);
          }
          sortedBlockInfoMap.put(mapId, sortedShuffleBlocks);
        }

        ((DirectBuffer) paddingBuf).cleaner().clean();

        writeIndex(sortedBlockInfoMap, indexFileName);
        updateSortedShuffleFiles(shuffleKey, fileId);
        if (!originFile.delete()) {
          logger.warn("clean origin file failed, origin file is : {}",
            originFile.getAbsolutePath());
        }
        logger.debug("sort complete for {} {}", shuffleKey, originFile.getName());
      } catch (Exception e) {
        logger.error("sort shuffle file {} error", originFile.getName(), e);
      } finally {
        sortingShuffleFiles.get(shuffleKey).remove(fileId);
      }
    }
  }
}
