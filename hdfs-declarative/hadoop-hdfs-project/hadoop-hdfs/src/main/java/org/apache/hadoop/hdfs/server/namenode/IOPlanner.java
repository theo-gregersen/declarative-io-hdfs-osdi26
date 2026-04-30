package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_SCHEDULING_DISPATCH_CLUSTERS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_SCHEDULING_DISPATCH_CLUSTERS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_SCHEDULING_RATE_RATIO_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_SCHEDULING_RATE_RATIO_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_SCRUBBING_RATE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_SCRUBBING_RATE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_REBALANCE_RATE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_REBALANCE_RATE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_RECONSTRUCTION_RATE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_RECONSTRUCTION_RATE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_CACHE_DELAY_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DECLARATIVE_CACHE_DELAY_MS_DEFAULT;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BLOCK_GROUP_INDEX_MASK;

/**
 * Represents an IO Planner for declarative IO requests.
 *
 * For EC, some declarations use group block ids (rebalance) while others use data block ids (e.g. scrubbing).
 * For replication, the block id can match but storage nodes are different.
 * We override LocatedBlock comparisons to consider both storage locations and ids (trimmed to group) to handle this.
 */
public class IOPlanner {
  public static final Logger LOG =
      LoggerFactory.getLogger(NameNode.class.getName());

  private final Metrics stats;

  private final ConcurrentLinkedQueue<Request> scheduledNonBlockingRequests;
  private final ReentrantLock pollingLock;

  private final BlockingQueue<Request> requests;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService workers;
  private final Runnable step;
  private final String dateFormatString;

  private ScheduledFuture<?> handle;
  private long period;
  private long maxIO;
  private final Map<String, Long> taskToRate;

  private final Configuration conf;
  private final FSNamesystem fs;
  private final List<DatanodeInfo> deadNodes;

  private final ConcurrentLinkedQueue<Set<CachedBlock>> cachedBlockSets;
  private final ConcurrentHashMap<Long, AtomicLong> cachedBlocks;
  private final DatanodeManager dm;

  private final long cacheDelayMs;
  private final CacheManager cacheManager;
  private final BlockManager blockManager;

  // Adding this to handle read-deletes that mess with scrubbing
  // Checking block manager might not work depending on frequency of block reports (~6hr by default)
  private final HashSet<Long> deletedBlocks;

  public IOPlanner(FSNamesystem fs) {
    scheduledNonBlockingRequests = new ConcurrentLinkedQueue<>();
    pollingLock = new ReentrantLock();
    requests = new LinkedBlockingQueue<>();
    scheduler = Executors.newScheduledThreadPool(1);
    workers = Executors.newFixedThreadPool(3);
    step = () -> {workers.submit(this::runOneRateScheduleIterationUpdated);};
    // DateFormat is not threadsafe, so just use the string as a field instead
    dateFormatString = "yyyy:MM:dd hh:mm:ss";
    taskToRate = new HashMap<>();
    conf = new HdfsConfiguration();
    stats = new Metrics();
    this.fs = fs;
    deadNodes = new ArrayList<>();
    cachedBlockSets = new ConcurrentLinkedQueue<>();
    cachedBlocks = new ConcurrentHashMap<>();
    dm = fs.getBlockManager().getDatanodeManager();
    cacheDelayMs = conf.getLong(DECLARATIVE_CACHE_DELAY_MS_KEY,
        DECLARATIVE_CACHE_DELAY_MS_DEFAULT);
    cacheManager = fs.getCacheManager();
    blockManager = fs.getBlockManager();
    deletedBlocks = new HashSet<>();
  }

  /**
   * Tell the cache management system to cache a set of LocatedBlock for a fixed period
   */
  private long cacheBlocks(Set<LocatedBlock> locatedBlocks) {
    // see Dispatcher DBLock getInternalBlock
    if (locatedBlocks.isEmpty()) return 0;
    long cacheBytes = 0;
    Set<CachedBlock> cachedBlockSet = new HashSet<>();
    for (LocatedBlock lb : locatedBlocks) {
      cacheBytes += lb.getBlockSize();
      for (int i = 0; i < lb.getStorageIDs().length; i++) {
        String storageId = lb.getStorageIDs()[i];
        DatanodeDescriptor dn = dm.getDatanode(storageId);
        // For EC blocks declared externally, the id is the group id, but we need the specific data block ids
        // See StripedBlockUtil constructInternalBlock and SequentialBlockGroupIdGenerator and LocatedStripedBlock
        // and BlockManager createLocatedBlock and BlockInfoStriped getBlockOnStorage
        // Grab correct internal block IDs for erasure coded blocks
        long internalId = lb.getBlock().getBlockId();
        if (lb.getBlock().getBlockId() < 0) {
          BlockInfo blockInfo = blockManager.getStoredBlock(lb.getBlock().getLocalBlock());
          if (blockInfo instanceof BlockInfoStriped) {
            BlockInfoStriped blockInfoStriped = (BlockInfoStriped) blockInfo;
            for (BlockInfoStriped.StorageAndBlockIndex storageAndBlockIndex : blockInfoStriped.getStorageAndIndexInfos()) {
              if (storageAndBlockIndex.getStorage().getStorageID().equals(storageId)) {
                long newInternalId = lb.getBlock().getBlockId() + storageAndBlockIndex.getBlockIndex();
                if (internalId + i != newInternalId) {
                  LOG.info("[IO Planner] fixed mismatched internal id: {} {} {}", internalId, internalId + i, newInternalId);
                }
                internalId = newInternalId;
                break;
              }
            }
          }
        }

        CachedBlock cb = new CachedBlock(internalId, (short)1, false);
        cachedBlockSet.add(cb);
        try {
          if (!cachedBlocks.containsKey(internalId)) {
            cachedBlocks.put(internalId, new AtomicLong(0));
          }
          cachedBlocks.get(internalId).getAndIncrement();
          cacheManager.cacheDeclarativeBlock(cb, dn);
        } catch (Exception e) {
          LOG.error("[IO Planner] failed to cache block: ", e);
        }
      }
    }
    cachedBlockSets.add(cachedBlockSet);
    return cacheBytes;
  }

  /**
   * Uncache blocks from oldest caching round
   */
  private void uncacheBlocks() {
    if (cachedBlockSets.isEmpty()) return;
    Set<CachedBlock> lastSet = cachedBlockSets.remove();
    for (CachedBlock cb : lastSet) {
      try {
        if (cachedBlocks.containsKey(cb.getBlockId())) {
          long countLeft = cachedBlocks.get(cb.getBlockId()).decrementAndGet();
          if (countLeft <= 0) {
            cachedBlocks.remove(cb.getBlockId());
            cacheManager.uncacheDeclarativeBlock(cb);
          }
        }
      } catch (Exception e) {
        LOG.error("[IO Planner] failed to uncache block: ", e);
      }
    }
  }

  /**
   * Set read bandwidth limits on certain tasks (beyond the general rate limit).
   * No per-task limits by default.
   */
  private void setTaskRates() {
    long scrubbingRate = conf.getLong(DECLARATIVE_SCRUBBING_RATE_KEY,
        DECLARATIVE_SCRUBBING_RATE_DEFAULT) * Math.round(Math.pow(2, 20));
    long rebalanceRate = conf.getLong(DECLARATIVE_REBALANCE_RATE_KEY,
        DECLARATIVE_REBALANCE_RATE_DEFAULT) * Math.round(Math.pow(2, 20));
    long reconstructionRate = conf.getLong(DECLARATIVE_RECONSTRUCTION_RATE_KEY,
        DECLARATIVE_RECONSTRUCTION_RATE_DEFAULT) * Math.round(Math.pow(2, 20));

    // Convert from B/s to B per window
    if (scrubbingRate != 0) {
      taskToRate.put(DeclarationType.SCRUBBING.name(), scrubbingRate * period);
    }
    if (rebalanceRate != 0) {
      taskToRate.put(DeclarationType.REBALANCE.name(), rebalanceRate * period);
    }
    if (reconstructionRate != 0) {
      taskToRate.put(DeclarationType.RECONSTRUCTION.name(), reconstructionRate * period);
    }

    LOG.info("[IO Planner] Setting rates (bytes per window): Scrubbing={}, Rebalance={}, Reconstruction={}",
        taskToRate.get(DeclarationType.SCRUBBING.name()), taskToRate.get(DeclarationType.REBALANCE.name()),
        taskToRate.get(DeclarationType.RECONSTRUCTION.name()));
  }

  /**
   * Run an iteration of scheduling.
   */
  private void runOneRateScheduleIterationUpdated() {
    long startTime = System.currentTimeMillis();

    // Clean up requests from dead datanodes
    try {
      DatanodeInfo[] datanodeInfos = fs.datanodeReport(HdfsConstants.DatanodeReportType.DEAD);
      for (DatanodeInfo datanodeInfo : datanodeInfos) {
        if (!deadNodes.contains(datanodeInfo)) {
          deadNodes.add(datanodeInfo);
          removeRequestsFromNode(datanodeInfo.getDatanodeUuid(), datanodeInfo.getDatanodeUuid());
        }
      }
    } catch (IOException e) {
      LOG.info("[IOPlanner] Caught exception while checking for dead Datanodes: {}", e.getMessage());
    }

    try {

      Map<String, Long> currentTaskTotals = new HashMap<>();
      Set<LocatedBlock> scheduledBlocks = new HashSet<>();
      Set<LocatedBlock> scheduledOverlappedBlocks = new HashSet<>();
      Set<Request> requestsInWindow = new HashSet<>();
      long maxBytes = maxIO;
      long totalBytes = 0;
      long dispatchesPerWindow = conf.getLong(DECLARATIVE_SCHEDULING_DISPATCH_CLUSTERS_KEY,
          DECLARATIVE_SCHEDULING_DISPATCH_CLUSTERS_DEFAULT);

      if (requests.isEmpty()) {
        LOG.info("[IO Planner] Nothing to schedule.");
        stats.queueMetrics(requests.size());
        stats.printTotalsToLogCSV();
        stats.printTaskTotalsToLogCSV();
        return;
      }

      // Need to clear partiallyScheduledBlocks and blockSetsSelected for partial requests from prior iteration
      for (Request r : requests) {
        if (r.doesAllowPartialCompletion()) {
          r.clearPartiallyScheduledBlocks();
        }
      }

      // Sort requests by rate
      Request[] batch = requests.toArray(new Request[0]);
      long currentTime = System.currentTimeMillis();
      Arrays.sort(batch, (r1, r2) -> Double.compare(r2.getRateForCompletion(currentTime), r1.getRateForCompletion(currentTime)));
      // Change order a bit to put more constrained requests earlier
      long curB = 0;
      int locali = 0;
      while (locali < batch.length) {
        Request r = batch[locali];
        long bytesNeeded = r.doesAllowPartialCompletion() ? (long) r.getRateForCompletion(currentTime) * period : r.estimateBytesNeeded();
        if (curB + bytesNeeded > maxBytes) {
          break;
        } else {
          curB += bytesNeeded;
          locali++;
        }
      }
      LOG.info("[IO Planner] found {} requests to order by constraint", locali);
      Arrays.sort(batch, 0, locali, (r1, r2) -> Double.compare(r2.estimateConstraint(), r1.estimateConstraint()));

      // First pass for up to rate needed
      for (int i = 0; i < batch.length; i++) {
        if (totalBytes > maxBytes) break;
        Request r = batch[i];
        totalBytes = considerRequest(totalBytes, maxBytes, r, currentTaskTotals, requestsInWindow,
            scheduledBlocks, scheduledOverlappedBlocks, true);
      }
      LOG.info("[IO Planner] scheduled {} non-free bytes in rate pass", totalBytes);
      long tmp = totalBytes;

      // Sort by deadline and then do idle scheduling
      batch = requests.toArray(new Request[0]);
      Arrays.sort(batch, Comparator.comparing(Request::getDeadline));
      // Second pass for idle
      for (int i = 0; i < batch.length; i++) {
        if (totalBytes > maxBytes) break;
        Request r = batch[i];
        totalBytes = considerRequest(totalBytes, maxBytes, r, currentTaskTotals, requestsInWindow,
            scheduledBlocks, scheduledOverlappedBlocks, false);
      }
      LOG.info("[IO Planner] scheduled {} non-free bytes in idle pass", totalBytes - tmp);
      tmp = totalBytes;

      // Third pass for overlap with idle
      batch = requests.toArray(new Request[0]);
      for (int i = 0; i < batch.length; i++) {
        Request r = batch[i];
        totalBytes = considerRequest(totalBytes, maxBytes, r, currentTaskTotals, requestsInWindow,
            scheduledBlocks, scheduledOverlappedBlocks, false);
      }

      // Make deep copy of requests for dispatching so that fields aren't updated once put in scheduledNonBlocking queue
      List<Request> deepCopies = new ArrayList<>();
      for (Request r : requestsInWindow) {
        deepCopies.add(r.getScheduledCopy());
      }

      List<List<Request>> dispatchClusters = getDispatchClusters(dispatchesPerWindow, deepCopies);
      LOG.info("[IO Planner] there are {} dispatch clusters", dispatchClusters.size());

      // Special check for read-deletes
      for (List<Request> cluster : dispatchClusters) {
        for (Request r : cluster) {
          if (r.getIoDeclarationType().equals("TRANSCODE")) {
            for (long l : r.getBlockSetsSelected()) {
              for (LocatedBlock lb : r.getBlockSets().get((int)l)) {
                long groupId = BlockType.fromBlockId(lb.getBlock().getBlockId()) == BlockType.STRIPED
                    ? lb.getBlock().getBlockId() & ~BLOCK_GROUP_INDEX_MASK : lb.getBlock().getBlockId();
                deletedBlocks.add(groupId);
              }
            }
          }
        }
      }

      // Set scheduled blocks (with overlap) to be cached
      long cacheBytes = cacheBlocks(scheduledOverlappedBlocks);

      long schedulingMs = System.currentTimeMillis() - startTime;

      // Wait a bit before dispatching for blocks to arrive in cache
      sleep(cacheDelayMs);
      // Uncache blocks from previous dispatch that don't need to stay in cache
      if (cachedBlockSets.size() > 2) uncacheBlocks();

      for (List<Request> cluster : dispatchClusters) {
        runOneDispatchRound(cluster);
        long waitTime = this.period / dispatchesPerWindow * 1000;
        try {
          if (dispatchesPerWindow > 1) {
            sleep(waitTime);
          }
        } catch (InterruptedException e) {
          LOG.info("[IO Planner] Interrupted during wait.");
        }
      }

      LOG.info("[IO Planner] Scheduling took {} ms", schedulingMs);
      LOG.info("[IO Planner] Tried to cache {} blocks ({} MB)", scheduledOverlappedBlocks.size(), cacheBytes / 1024 / 1024);
      stats.queueMetrics(requests.size());
      stats.printTotalsToLogCSV();
      stats.printTaskTotalsToLogCSV();
      stats.printMinimumBytesReadToLog();

      List<Request> requestsToIterate = new ArrayList<>(deepCopies);
      while (!requestsToIterate.isEmpty()) {
        Request r1 = requestsToIterate.get(0);
        for (int i = 1; i < requestsToIterate.size(); i++) {
          Request r2 = requestsToIterate.get(i);
          if (!r1.getUuid().equals(r2.getUuid())) { // Need this check since requests can be added multiple times (not anymore i think)
            checkPairwise(r1, r2);
          }
        }
        requestsToIterate.remove(0);
      }
      stats.printPairwiseOverlaps();

    } catch (Exception e) {
      LOG.error("[IO Planner] Caught exception during scheduling step:", e);
    }
  }

  /**
   * Schedule a request if possible
   * @return total + number of bytes scheduled for the request r
   */
  private long considerRequest(long total, long max, Request r, Map<String, Long> currentTaskTotals, Set<Request> requestsInWindow,
                               Set<LocatedBlock> scheduledBlocks, Set<LocatedBlock> scheduledOverlappedBlocks, boolean rateLimit) {
    long taskScheduled = currentTaskTotals.getOrDefault(r.getIoDeclarationType(), 0L);
    long foundFreeBlocks = 0;
    long foundfreeB = 0;
    long foundNonFreeBlocks = 0;
    long foundNonfreeB = 0;

    // This might cause weird things if the request is overdue since rate will be max long value
    long rateLimitBytes = (long) r.getRateForCompletion(System.currentTimeMillis()) * period;

    if (!r.isLoggedMissed() && r.isMissed()) {
      r.setLoggedMissed();
      stats.missedDeadline(r);
    }

    // For partial selection, keep the request in the queue but update block sets needed and block sets.
    // Greedily select as many block sets as we can.
    if (r.doesAllowPartialCompletion() && r.blockSets.size() == 1) {
      // In this case, we can schedule individual blocks rather than block sets
      // Don't need the findOverlap scaling because the only things that schedule like this should be blocks
      // with one storage location, e.g. balancing and scrubbing.
      try {
        Set<LocatedBlock> selected = new HashSet<>();

        // First loop to find free blocks (ignore max and rate limit)
        long blocksToSchedule = r.getBlockSetsNeeded();
        long bytesToSchedule = r.scheduleByBytes() ? r.getBytesNeeded() : Long.MAX_VALUE;
        Iterator<LocatedBlock> blockIt = r.getBlockSets().get(0).iterator();
        while (blockIt.hasNext() && foundFreeBlocks < blocksToSchedule && foundfreeB < bytesToSchedule) {
          LocatedBlock lb = blockIt.next();
          if (scheduledBlocks.contains(lb)) {
            // Don't need to account for number of storage IDs because scrubbing/rebalancing only have one
            selected.add(lb);
            blockIt.remove();
            foundFreeBlocks++;
            foundfreeB += lb.getBlockSize();
          }
        }
        Set<LocatedBlock> overlapSelected = new HashSet<>(selected);

        // Second loop to get remaining blocks (up to max or rate limit)
        blocksToSchedule = blocksToSchedule - foundFreeBlocks;
        long byteLimit = max - total;
        if (rateLimit) {
          byteLimit = Math.min(byteLimit,
              taskToRate.getOrDefault(r.getIoDeclarationType(), Long.MAX_VALUE) - taskScheduled);
          byteLimit = Math.min(byteLimit, rateLimitBytes - foundfreeB);
        }
        if (r.scheduleByBytes()) {
          byteLimit = Math.min(byteLimit, r.getBytesNeeded() - foundfreeB);
        }
        blockIt = r.getBlockSets().get(0).iterator();
        while (blockIt.hasNext() && foundNonFreeBlocks < blocksToSchedule && foundNonfreeB < byteLimit) {
          LocatedBlock lb = blockIt.next();
          // Special check for deleted blocks (mainly for scrubbing)
          long groupId = BlockType.fromBlockId(lb.getBlock().getBlockId()) == BlockType.STRIPED
              ? lb.getBlock().getBlockId() & ~BLOCK_GROUP_INDEX_MASK : lb.getBlock().getBlockId();
          if (deletedBlocks.contains(groupId) && r.getIoDeclarationType().equals(DeclarationType.SCRUBBING.name())) {
            blockIt.remove();
            stats.addDeletedBytes(lb.getBlock().getBlockId() < 0 ? lb.getBlockSize() / lb.getStorageIDs().length : lb.getBlockSize());
            r.setBlockSetsNeeded(r.getBlockSetsNeeded() - 1);
            continue;
          }
          // Don't need to account for number of storage IDs because scrubbing/rebalancing only have one
          selected.add(lb);
          blockIt.remove();
          foundNonFreeBlocks++;
          foundNonfreeB += lb.getBlockSize();
        }

        long blocksFound = foundFreeBlocks + foundNonFreeBlocks;
        long bytesFound = foundfreeB + foundNonfreeB;
        if (blocksFound != 0) {
          // This might double number of times scheduled if a request is scheduled multiple times
          stats.recordScheduledRequest(r, foundFreeBlocks, foundNonFreeBlocks, foundfreeB, foundNonfreeB);
          total += foundNonfreeB;
          r.addPartiallyScheduledBlocks(selected);
          r.setBlockSetsNeeded(r.getBlockSetsNeeded() - blocksFound);
          if (r.scheduleByBytes()) {
            r.setBytesNeeded(r.getBytesNeeded() - bytesFound);
          }
          currentTaskTotals.put(r.getIoDeclarationType(), taskScheduled + foundNonfreeB);
          scheduledBlocks.addAll(selected);
          scheduledOverlappedBlocks.addAll(overlapSelected);
          requestsInWindow.add(r); // Might be added twice, should be ok though since same reference
        }

        if (r.getBlockSetsNeeded() == 0 || (r.scheduleByBytes() && r.getBytesNeeded() <= 0)) {
          boolean removed = requests.remove(r);
          if (!removed) {
            LOG.error("[IO Planner] Failed to remove {} from the queue", r);
          }
          stats.removedFromQueue(r);
        }
      } catch (Exception e) {
        LOG.error("[IO Planner] Caught exception when scheduling partial request {}, will dispatch without scheduling: ", r, e);
        boolean removed = requests.remove(r);
        if (!removed) {
          LOG.error("[IO Planner] Failed to remove {} from the queue", r);
        }
        stats.removedFromQueue(r);
        requestsInWindow.add(r);
      }
    } else {
      // In this case, we need to schedule entire block sets
      try {
        Set<LocatedBlock> overlapSelected = new HashSet<>();
        List<Integer> blockSetsSelected = new ArrayList<>();
        Set<Long> blockSetsAlreadyScheduled = new HashSet<>(r.getBlockSetsAlreadyScheduled());
        // First loop to see if we can get any block sets for free
        for (int i = 0; i < r.blockSets.size() && blockSetsSelected.size() < r.getBlockSetsNeeded(); i++) {
          if (blockSetsAlreadyScheduled.contains((long)i)) continue;
          Set<LocatedBlock> blocks = new HashSet<>(r.getBlockSets().get(i));
          long setFreeBlocks = 0;
          long setFreeBytes = 0;
          boolean isFree = true;

          for (LocatedBlock lb : blocks) {
            long freeBlocks = findOverlap(scheduledBlocks, lb);
            long nonFreeBlocks = lb.getStorageIDs().length - freeBlocks;
            if (nonFreeBlocks != 0) {
              isFree = false;
              break;
            }
            setFreeBlocks += freeBlocks;
            // Doby check to scale striped block size appropriately
            setFreeBytes += freeBlocks * (lb.getBlock().getBlockId() < 0
                ? lb.getBlockSize() / lb.getStorageIDs().length : lb.getBlockSize());
          }

          if (isFree) {
            foundFreeBlocks += setFreeBlocks;
            foundfreeB += setFreeBytes;
            blockSetsSelected.add(i);
            overlapSelected.addAll(blocks);
          }
        }

        long byteLimit = max - total;
        if (rateLimit && r.doesAllowPartialCompletion()) {
          byteLimit = Math.min(byteLimit,
              taskToRate.getOrDefault(r.getIoDeclarationType(), Long.MAX_VALUE) - taskScheduled);
          byteLimit = Math.min(byteLimit, rateLimitBytes - foundfreeB);
        }

        // Using a heuristic: loop linearly through block sets and see if we can hit block sets needed within max
        // and rate limit; if so, schedule
        for (int i = 0; i < r.getBlockSets().size() && blockSetsSelected.size() < r.getBlockSetsNeeded() && foundNonfreeB < byteLimit; i++) {
          if (blockSetsAlreadyScheduled.contains((long)i) || blockSetsSelected.contains(i)) continue;
          Set<LocatedBlock> blocks = new HashSet<>(r.getBlockSets().get(i));
          long setFreeBlocks = 0;
          long setFreeBytes = 0;
          long setNonFreeBlocks = 0;
          long setNonFreeBytes = 0;

          Set<LocatedBlock> tmpOverlap = new HashSet<>();
          for (LocatedBlock lb : blocks) {
            long freeBlocks = findOverlap(scheduledBlocks, lb);
            long nonFreeBlocks = lb.getStorageIDs().length - freeBlocks;
            setFreeBlocks += freeBlocks;
            setNonFreeBlocks += nonFreeBlocks;
            // Dividing by length is ok for reconstruction since HDFS specifies sources/target when creating EC recon work
            setFreeBytes += freeBlocks * (lb.getBlock().getBlockId() < 0 ? lb.getBlockSize() / lb.getStorageIDs().length : lb.getBlockSize());
            setNonFreeBytes += nonFreeBlocks * (lb.getBlock().getBlockId() < 0 ? lb.getBlockSize() / lb.getStorageIDs().length : lb.getBlockSize());
            if (freeBlocks > 0) tmpOverlap.add(lb);
          }

          if (foundNonfreeB + setNonFreeBytes <= byteLimit) {
            foundFreeBlocks += setFreeBlocks;
            foundfreeB += setFreeBytes;
            foundNonFreeBlocks += setNonFreeBlocks;
            foundNonfreeB += setNonFreeBytes;
            blockSetsSelected.add(i);
            overlapSelected.addAll(tmpOverlap);
          }
        }

        if ((blockSetsSelected.size() == r.getBlockSetsNeeded() || (!blockSetsSelected.isEmpty() && r.doesAllowPartialCompletion())) &&
            foundNonfreeB <= byteLimit) {
          stats.recordScheduledRequest(r, foundFreeBlocks, foundNonFreeBlocks, foundfreeB, foundNonfreeB);
          total += foundNonfreeB;
          currentTaskTotals.put(r.getIoDeclarationType(), taskScheduled + foundNonfreeB);
          for (int i : blockSetsSelected) {
            scheduledBlocks.addAll(r.getBlockSets().get(i));
            r.getBlockSetsSelected().add((long) i);
            r.getBlockSetsAlreadyDispatched().add((long) i);
          }
          long blockSetsNeeded = r.getBlockSetsNeeded();
          r.setBlockSetsNeeded(blockSetsNeeded - blockSetsSelected.size());
          requestsInWindow.add(r);
          if (r.getBlockSetsNeeded() == 0) {
            boolean removed = requests.remove(r);
            if (!removed) {
              LOG.error("[IO Planner] Failed to remove {} from the queue", r);
            }
            stats.removedFromQueue(r);
          }
          scheduledOverlappedBlocks.addAll(overlapSelected);
        }
      } catch (Exception e) {
        LOG.error("[IO Planner] Caught exception when scheduling non-partial request {}, will dispatch without scheduling: ", r, e);
        boolean removed = requests.remove(r);
        if (!removed) {
          LOG.error("[IO Planner] Failed to remove {} from the queue", r);
        }
        stats.removedFromQueue(r);
        requestsInWindow.add(r);
      }
    }

    return total;
  }

  /**
   * Check the scheduled block overlap between two requests.
   */
  private void checkPairwise(Request r1, Request r2) {
    long bytes = 0;
    String[] requestTypes = new String[]{r1.getIoDeclarationType(), r2.getIoDeclarationType()};
    Arrays.sort(requestTypes);
    String key = requestTypes[0] + requestTypes[1];
    Set<LocatedBlock> lbs = new HashSet<>();
    for (long i : r1.getBlockSetsSelected()) {
      lbs.addAll(r1.getBlockSets().get((int)i));
    }
    lbs.addAll(r1.getPartiallyScheduledBlocks());
    Set<LocatedBlock> lbs2 = new HashSet<>();
    for (long i : r2.getBlockSetsSelected()) {
      lbs2.addAll(r2.getBlockSets().get((int) i));
    }
    lbs2.addAll(r2.getPartiallyScheduledBlocks());
    for (LocatedBlock lb : lbs2) {
      long overlap = findOverlap(lbs, lb);
      bytes += overlap * (lb.getBlock().getBlockId() < 0
          ? lb.getBlockSize() / lb.getStorageIDs().length : lb.getBlockSize());
    }
    if (bytes != 0) {
      stats.addPairwiseOverlap(key, bytes);
    }
  }

  /**
   * Run one dispatch for the current window.
   */
  private void runOneDispatchRound(List<Request> cluster) {
    for (Request r : cluster) {
      if (r.isBlocking()) {
        r.complete(0);
      } else {
        addScheduledRequest(r);
      }
    }
  }

  /**
   * Calculate request clusters for dispatch in the window.
   * @return list of request clusters (one entry per cluster)
   */
  private List<List<Request>> getDispatchClusters(long nClusters, List<Request> requestsInWindow) {
    List<List<Request>> clusters = new ArrayList<>();
    List<Set<LocatedBlock>> clusterBlocks = new ArrayList<>();

    // Current bottom-up agglomerative clustering approach is roughly O(N^2 * logN) for N requests in the window
    // At the moment, not checking for even rates across clusters
    // There are a lot of heuristics being used in this simple implementation

    if (requestsInWindow.size() < nClusters) {
      for (Request r : requestsInWindow) {
        List<Request> l = new ArrayList<>();
        l.add(r);
        clusters.add(l);
      }
      return clusters;
    }

    // Start with one non-per-block completable request per cluster
    // This might separate overlapping requests... also need to update for partial with blocksets, not just scrubbing, etc.
    int nonpartials = 0;
    for (Request r : requestsInWindow) {
      if (r.doesAllowPartialCompletion() && r.getBlockSets().size() == 1) {
        continue;
      }
      nonpartials++;
      List<Request> rList = new ArrayList<>();
      rList.add(r);
      clusters.add(rList);
      Set<LocatedBlock> blocks = new HashSet<>();
      for (long i : r.getBlockSetsSelected()) {
        blocks.addAll(r.getBlockSets().get((int) i));
      }
      clusterBlocks.add(blocks);
    }

    if (nonpartials > nClusters) {
      // Merge until nClusters
      while (clusters.size() != nClusters) {
        List<List<Request>> newClusters = new ArrayList<>();
        List<Set<LocatedBlock>> newClusterBlocks = new ArrayList<>();
        while (clusters.size() + newClusters.size() != nClusters
            && !clusterBlocks.isEmpty()) {
          Set<LocatedBlock> curBlocks = clusterBlocks.get(0);
          List<Request> curRequests = clusters.get(0);
          clusters.remove(0);
          clusterBlocks.remove(0);
          if (clusterBlocks.isEmpty()) {
            // If nothing to match with, just add this cluster
            newClusters.add(curRequests);
            newClusterBlocks.add(curBlocks);
            break;
          }
          int bestIndex = 0;
          long bestFreeBlocks = 0;
          for (int i = 0; i < clusterBlocks.size(); i++) {
            Set<LocatedBlock> blocks = new HashSet<>(curBlocks);
            long curFreeBlocks = 0;
            for (LocatedBlock lb : clusterBlocks.get(i)) {
              curFreeBlocks += findOverlap(blocks, lb);
            }
            if (curFreeBlocks > bestFreeBlocks) {
              bestFreeBlocks = curFreeBlocks;
              bestIndex = i;
            }
          }
          Set<LocatedBlock> bestBlocks = clusterBlocks.get(bestIndex);
          List<Request> bestCluster = clusters.get(bestIndex);
          clusterBlocks.remove(bestIndex);
          clusters.remove(bestIndex);
          List<Request> newCluster = new ArrayList<>(curRequests);
          newCluster.addAll(bestCluster);
          Set<LocatedBlock> newClusterBlock = new HashSet<>(curBlocks);
          newClusterBlock.addAll(bestBlocks);
          newClusters.add(newCluster);
          newClusterBlocks.add(newClusterBlock);
        }
        if (clusters.size() + newClusters.size() == nClusters) {
          clusters.addAll(newClusters);
          clusterBlocks.addAll(newClusterBlocks); // was not having this the bug?
        } else {
          clusters = newClusters;
          clusterBlocks = newClusterBlocks;
        }
      }
    } else {
      while (clusters.size() < nClusters) {
        List<Request> l = new ArrayList<>();
        clusters.add(l);
        Set<LocatedBlock> blocks = new HashSet<>();
        clusterBlocks.add(blocks);
      }
    }

    // Split the partially completable requests to the clusters with overlap
    // For non-overlapping blocks, put them in cluster with least blocks
    // Can add req multiple times to one cluster, but should get merged later
    for (Request r : requestsInWindow) {
      if (r.doesAllowPartialCompletion() && r.getBlockSets().size() == 1) {
        Request rCpy = r.getScheduledCopy();
        for (int i = 0; i < clusterBlocks.size(); i++) {
          Set<LocatedBlock> rSubset = new HashSet<>();
          for (LocatedBlock lb : rCpy.getPartiallyScheduledBlocks()) {
            long free = findOverlap(clusterBlocks.get(i), lb);
            if (free > 0) {
              rSubset.add(lb);
            }
          }
          if (!rSubset.isEmpty()) {
            rCpy.getPartiallyScheduledBlocks().removeAll(rSubset);
            clusterBlocks.get(i).addAll(rSubset);
            Request rCpy2 = rCpy.getScheduledCopy();
            rCpy2.clearPartiallyScheduledBlocks();
            rCpy2.addPartiallyScheduledBlocks(rSubset);
            clusters.get(i).add(rCpy2);
          }
        }
        if (!rCpy.getPartiallyScheduledBlocks().isEmpty()) {
          // clusterBlocks might be empty if there were no non-partial requests...
          if (clusterBlocks.isEmpty()) {
            Set<LocatedBlock> localBlocks = new HashSet<>();
            localBlocks.addAll(rCpy.getPartiallyScheduledBlocks());
            clusterBlocks.add(localBlocks);
            List<Request> newCluster = new ArrayList<>();
            newCluster.add(rCpy);
            clusters.add(newCluster);
          } else {
            int best = 0;
            int bestCount = Integer.MAX_VALUE;
            for (int i = 0; i < clusterBlocks.size(); i++) {
              if (clusterBlocks.get(i).size() < bestCount) {
                best = i;
                bestCount = clusterBlocks.get(i).size();
              }
            }
            clusterBlocks.get(best).addAll(rCpy.getPartiallyScheduledBlocks());
            clusters.get(best).add(rCpy);
          }

        }
      }
    }

    return clusters;
  }

  /**
   * Start (or restart) the scheduler.
   * @param period time in seconds per scheduler step
   * @param maxIO maximum IO in MB/s that the scheduler can schedule
   */
  public void startScheduler(long period, long maxIO) {
    LOG.info("[IO Planner] Starting the scheduler with a period of {}s and max IO of {} MB/s", period, maxIO);
    if (handle != null) handle.cancel(true);
    handle = scheduler.scheduleAtFixedRate(step, 0, period, TimeUnit.SECONDS);
    this.maxIO = maxIO * 1024 * 1024 * period;
    this.period = period;
    setTaskRates();
  }

  /**
   * Declare IO to the scheduler. Will be added to scheduledNonBlockingRequests when scheduled.
   * Returns the random UUID of the newly created Request.
   */
  public UUID declare(String ioDeclarationType, List<List<LocatedBlock>> blockSets, long blockSetsNeeded,
                      String deadline, boolean allowPartialCompletion)
      throws InterruptedException, ParseException {
    Date parsedDeadline;
    DateFormat dateFormat = new SimpleDateFormat(dateFormatString);
    try {
      parsedDeadline = dateFormat.parse(deadline);
    } catch (ParseException e) {
      parsedDeadline = new Date(Long.parseLong(deadline));
    }

    Request req = new Request(ioDeclarationType, null, blockSets, blockSetsNeeded, parsedDeadline, false,
        allowPartialCompletion);
    LOG.info("[IO Planner] Received non-blocking declaration {}", req);
    stats.declarationMetrics(req);

    requests.put(req);
    return req.getUuid();
  }

  /**
   * Declare IO from a cluster location. Will be added to scheduledNonBlockingRequests when scheduled.
   * Returns the random UUID of the newly created Request.
   */
  public UUID declarePhysical(String ioDeclarationType, List<List<LocatedBlock>> blockSets, long blockSetsNeeded, String deadline,
                              String storageId, String datanodeUuid, DatanodeRegistration datanodeRegistration)
      throws InterruptedException, ParseException {
    return declarePhysical(ioDeclarationType, blockSets, blockSetsNeeded, deadline,
        storageId, datanodeUuid, datanodeRegistration, -1);
  }

  /**
   * Special declaration version with extra parameters for balancer requests.
   */
  public UUID declarePhysical(String ioDeclarationType, List<List<LocatedBlock>> blockSets, long blockSetsNeeded, String deadline,
                              String storageId, String datanodeUuid, DatanodeRegistration datanodeRegistration, long bytesNeeded)
      throws InterruptedException, ParseException {
    Date parsedDeadline;
    DateFormat dateFormat = new SimpleDateFormat(dateFormatString);
    try {
      parsedDeadline = dateFormat.parse(deadline);
    } catch (ParseException e) {
      parsedDeadline = new Date(Long.parseLong(deadline));
    }

    // For now, just a manual check for scrubbing and rebalance.
    boolean allowPartialCompletion = ioDeclarationType.equals(DeclarationType.SCRUBBING.name())
        || ioDeclarationType.equals(DeclarationType.REBALANCE.name());

    Request req = new Request(ioDeclarationType, null, blockSets, blockSetsNeeded, parsedDeadline, false,
        storageId, datanodeUuid, datanodeRegistration, allowPartialCompletion, bytesNeeded);
    stats.declarationMetrics(req);

    requests.put(req);
    return req.getUuid();
  }

  /**
   * Declare IO to the scheduler. Returns when the IO is scheduled to be executed.
   * WARNING: this method is blocking.
   */
  public List<Long> declareAndReturnOnSchedule(String ioDeclarationType, List<List<LocatedBlock>> blockSets, long blockSetsNeeded, String deadline)
      throws InterruptedException, ParseException {
    Date parsedDeadline;
    DateFormat dateFormat = new SimpleDateFormat(dateFormatString);
    try {
      parsedDeadline = dateFormat.parse(deadline);
    } catch (ParseException e) {
      parsedDeadline = new Date(Long.parseLong(deadline));
    }
    CompletableFuture<Long> callback = new CompletableFuture<>();
    // Blocking requests can't be partially completable.
    boolean allowPartialCompletion = false;
    Request req = new Request(ioDeclarationType, callback, blockSets, blockSetsNeeded, parsedDeadline, true,
        allowPartialCompletion);
    stats.declarationMetrics(req);

    requests.put(req);

    try {
      callback.get();
      return new ArrayList<>(req.getBlockSetsSelected());
    } catch (Exception e) {
      // Request failed, so should remove it.
      LOG.error("[IO Planner] interrupted during blocking declaration, removing request {}", req);
      requests.remove(req);
      stats.removedFromQueue(req);
      throw new InterruptedException();
    }
  }

  /**
   * Add an async request to the queue for dispatching.
   */
  private void addScheduledRequest(Request req) {
    try {
      pollingLock.lock();
      for (Request r : scheduledNonBlockingRequests) {
        // Update if it was already there
        if (req.getUuid().equals(r.getUuid())) {
          LOG.info("[IO Planner] addScheduledRequest updating {} with {}", r, req);
          int before = r.getPartiallyScheduledBlocks().size();
          r.getPartiallyScheduledBlocks().addAll(req.getPartiallyScheduledBlocks());
          LOG.info("[IO Planner] had {} partiallyScheduledBlocks, now has {}", before, r.getPartiallyScheduledBlocks().size());
          for (long i : req.getBlockSetsSelected()) {
            if (!r.getBlockSetsSelected().contains(i)) {
              r.getBlockSetsSelected().add(i);
            }
          }
          return;
        }
      }
      scheduledNonBlockingRequests.add(req);
      stats.scheduledQueueMetrics(scheduledNonBlockingRequests.size());
    } finally {
      pollingLock.unlock();
    }
  }

  /**
   * Used to poll the dispatch queue for async requests to see if they are scheduled.
   */
  public List<Request> pollDeclarationType(String declarationType) {
    List<Request> requestCopies = new ArrayList<>();
    pollingLock.lock();
    try {
      Iterator<Request> itr = scheduledNonBlockingRequests.iterator();
      while (itr.hasNext()) {
        Request r = itr.next();
        if (r.getIoDeclarationType().equals(declarationType)) {
          requestCopies.add(r);
          itr.remove();
        }
      }
    } finally {
      pollingLock.unlock();
    }
    return requestCopies;
  }

  /**
   * Used to poll the dispatch queue for async requests to see if they are scheduled.
   */
  public List<Request> pollDeclarationType(String declarationType, String datanodeUuid) {
    List<Request> requestCopies = new ArrayList<>();
    pollingLock.lock();
    try {
      Iterator<Request> itr = scheduledNonBlockingRequests.iterator();
      while (itr.hasNext()) {
        Request r = itr.next();
        if (r.getIoDeclarationType().equals(declarationType) && r.getDatanodeUuid().equals(datanodeUuid)) {
          requestCopies.add(r);
          itr.remove();
        }
      }
    } finally {
      pollingLock.unlock();
    }
    return requestCopies;
  }

  /**
   * Used to poll the dispatch queue for async requests to see if they are scheduled.
   */
  public List<Request> pollDeclarationType(String declarationType, DatanodeRegistration datanodeRegistration) {
    List<Request> requestCopies = new ArrayList<>();
    pollingLock.lock();
    try {
      Iterator<Request> itr = scheduledNonBlockingRequests.iterator();
      while (itr.hasNext()) {
        Request r = itr.next();
        if (r.getIoDeclarationType().equals(declarationType) && r.getDatanodeRegistration().equals(datanodeRegistration)) {
          requestCopies.add(r);
          itr.remove();
        }
      }
    } finally {
      pollingLock.unlock();
    }
    return requestCopies;
  }

  /**
   * Check if requestUuid is in scheduled requests and return scheduled blocks/sets if so.
   * Remove from scheduledNonBlockingRequests if there is a match
   */
  public List<Long> pollDeclaration(String requestUuid) {
    for (Request req : scheduledNonBlockingRequests) {
      if (req.getUuid().toString().equals(requestUuid)) {
        removeScheduledRequest(req);
        return req.getBlockSetsSelected();
      }
    }
    return new ArrayList<>();
  }

  /**
   * Remove a request from the list of (non-blocking) scheduled requests
   * @param request request to remove
   */
  public void removeScheduledRequest(Request request) {
    Request inQueue = null;
    for (Request r : scheduledNonBlockingRequests) {
      if (r.getUuid() == request.getUuid()) {
        inQueue = r;
        break;
      }
    }
    if (inQueue == null) return;
    if (request.getPartiallyScheduledBlocks().size() != inQueue.getPartiallyScheduledBlocks().size()) {
      LOG.info("[IO Planner] partially scheduled diff found, so not removing from scheduled queue yet");
      if (request.getPartiallyScheduledBlocks().size() > inQueue.getPartiallyScheduledBlocks().size()) {
        LOG.info("[IO Planner] partially scheduled copy has more items than in queue");
      }
      inQueue.getPartiallyScheduledBlocks().removeAll(request.getPartiallyScheduledBlocks());
    } else if (request.getBlockSetsSelected().size() != inQueue.getBlockSetsSelected().size()) {
      LOG.info("[IO Planner] scheduled diff found, so not removing from scheduled queue yet");
      if (request.getBlockSetsSelected().size() > inQueue.getBlockSetsSelected().size()) {
        LOG.info("[IO Planner] scheduled copy has more items than in queue");
      }
      inQueue.getBlockSetsSelected().removeAll(request.getBlockSetsSelected());
    } else {
      scheduledNonBlockingRequests.remove(inQueue);
      stats.scheduledQueueMetrics(scheduledNonBlockingRequests.size());
    }
  }

  /**
   * Remove all requests from the scheduled queue and request queue that match the node identifying information.
   */
  private void removeRequestsFromNode(String storageId, String datanodeUuid) {
    LOG.info("[IOPlanner] Removing requests from {} ({})", storageId, datanodeUuid);
    Iterator<Request> it = requests.iterator();
    while (it.hasNext()) {
      Request r = it.next();
      // Special check for rebalance since it uses the storage ID differently...
      if (r.getIoDeclarationType().equals(DeclarationType.REBALANCE.name())) continue;
      if (r.getStorageId().equals(storageId) || r.getDatanodeUuid().equals(datanodeUuid)) {
        long bytesRemoved = 0;
        long blocksRemoved = 0;
        for (Set<LocatedBlock> l : r.getBlockSets()) {
          for (LocatedBlock lb : l) {
            bytesRemoved += lb.getBlockSize();
            blocksRemoved++;
          }
        }
        LOG.info("[IOPlanner] Removed {} bytes, {} blocks, of queued request ({}) from {} ({})", bytesRemoved, blocksRemoved, r.getIoDeclarationType(), storageId, datanodeUuid);
        it.remove();
      }
    }
    it = scheduledNonBlockingRequests.iterator();
    while (it.hasNext()) {
      Request r = it.next();
      // Special check for rebalance since it uses the storage ID differently...
      if (r.getIoDeclarationType().equals(DeclarationType.REBALANCE.name())) continue;
      if (r.getStorageId().equals(storageId) || r.getDatanodeUuid().equals(datanodeUuid)) {
        long bytesRemoved = 0;
        long blocksRemoved = 0;
        if (r.doesAllowPartialCompletion()) {
          for (LocatedBlock lb : r.getPartiallyScheduledBlocks()) {
            bytesRemoved += lb.getBlockSize();
            blocksRemoved++;
          }
          for (long i : r.getBlockSetsSelected()) {
            for (LocatedBlock lb : r.getBlockSets().get((int) i)) {
              bytesRemoved += lb.getBlockSize();
              blocksRemoved++;
            }
          }
        } else {
          for (long l : r.getBlockSetsSelected()) {
            for (LocatedBlock lb : r.getBlockSets().get((int)l)) {
              bytesRemoved += lb.getBlockSize();
              blocksRemoved++;
            }
          }
        }
        LOG.info("[IOPlanner] Removed {} bytes, {} blocks, of scheduled request ({}) from {} ({})", bytesRemoved, blocksRemoved, r.getIoDeclarationType(), storageId, datanodeUuid);
        it.remove();
      }
    }
    stats.scheduledQueueMetrics(scheduledNonBlockingRequests.size());
  }

  /**
   * Determine potential overlap between a set of blocks and a block.
   */
  private long findOverlap(Set<LocatedBlock> lbs, LocatedBlock lb) {
    if (lbs.contains(lb)) {
      long overlap = 0;
      if (lb.getStorageIDs().length > 1) {
        for (String lid : lb.getStorageIDs()) {
          // Slightly breaking LocatedBlock semantics, but only care about storageID and block
          LocatedBlock lbPartial = new LocatedBlock(lb.getBlock(), 
              new DatanodeInfoWithStorage[]{lb.getLocations()[0]},
              new String[]{lid}, lb.getStorageTypes());
          if (lbs.contains(lbPartial)) overlap++;
        }
      } else {
        overlap = 1;
      }
      return overlap;
    }
    return 0;
  }

  public enum DeclarationType {
    SCRUBBING,
    RECONSTRUCTION,
    REBALANCE,
    USER
  }

  /**
   * Represents an IO declaration request.
   */
//  public static class Request implements Comparable<Request> {
  public static class Request {
    private final String ioDeclarationType;
    private final CompletableFuture<Long> callback;
    // For requests that allow partial completion, this should have one entry
    private final List<Set<LocatedBlock>> blockSets;
    // For requests that allow partial completion, this represents blocks needed, not block sets
    private long blockSetsNeeded;
    private long bytesNeeded; // this is currently only used for balancer requests
    private final boolean scheduleByBytes;
    private final List<Long> blockSetsSelected; // this is for a given scheduling period
    private final List<Long> blockSetsAlreadyDispatched;
    private final Date declaredAt;
    private final Date deadline;
    private final boolean blocking;
    private final UUID uuid;
    private boolean loggedMissed;
    private long averageBlockBytes;

    // At the moment, this is only for internal non-blocking workloads: scrubbing, rebalance
    // These will have one block set, and blockSetsNeeded corresponds to how many blocks in that set
    private final boolean allowPartialCompletion;
    private final Set<LocatedBlock> partiallyScheduledBlocks;

    // Physical locations for integration with scrubbing and balancer
    private final DatanodeRegistration datanodeRegistration;
    private final String storageId;
    private final String datanodeUuid;

    private long calculateAverageBlockSize(List<Set<LocatedBlock>> blockSets) {
      long total = 0;
      long count = 0;
      for (Set<LocatedBlock> blocks : blockSets) {
        for (LocatedBlock locatedBlock : blocks) {
          total += locatedBlock.getBlockSize();
          count++;
        }
      }
      if (count == 0) return 0;
      return total / count;
    }

    // This constructor is intended for virtual blocks
    public Request(String ioDeclarationType, CompletableFuture<Long> callback, List<List<LocatedBlock>> blockSets,
                   long blockSetsNeeded, Date deadline, boolean blocking, boolean allowPartialCompletion) {
      this.declaredAt = new Date(System.currentTimeMillis());
      this.ioDeclarationType = ioDeclarationType;
      this.callback = callback;
      List<Set<LocatedBlock>> converted = new ArrayList<>();
      for (List<LocatedBlock> blockSet : blockSets) {
        converted.add(new HashSet<>(blockSet));
      }
      this.blockSets = converted;
      this.blockSetsNeeded = blockSetsNeeded;
      this.bytesNeeded = -1;
      this.scheduleByBytes = false;
      this.blockSetsSelected = new ArrayList<>();
      this.blockSetsAlreadyDispatched = new ArrayList<>();
      this.deadline = deadline;
      this.blocking = blocking;
      this.uuid = UUID.randomUUID();
      this.datanodeRegistration = null;
      this.storageId = "";
      this.datanodeUuid = "";
      this.allowPartialCompletion = allowPartialCompletion;
      this.partiallyScheduledBlocks = new HashSet<>();
      this.loggedMissed = false;
      this.averageBlockBytes = calculateAverageBlockSize(this.blockSets);
    }

    // This constructor is intended for physical blocks (i.e., have a specified location)
    public Request(String ioDeclarationType, CompletableFuture<Long> callback, List<List<LocatedBlock>> blockSets,
                   long blockSetsNeeded, Date deadline, boolean blocking, String storageId, String datanodeUuid,
                   DatanodeRegistration datanodeRegistration, boolean allowPartialCompletion, long bytesNeeded) {
      this.declaredAt = new Date(System.currentTimeMillis());
      this.ioDeclarationType = ioDeclarationType;
      this.callback = callback;
      List<Set<LocatedBlock>> converted = new ArrayList<>();
      for (List<LocatedBlock> blockSet : blockSets) {
        converted.add(new HashSet<>(blockSet));
      }
      this.blockSets = converted;
      this.blockSetsNeeded = blockSetsNeeded;
      this.bytesNeeded = bytesNeeded;
      this.scheduleByBytes = bytesNeeded > 0;
      this.blockSetsSelected = new ArrayList<>();
      this.blockSetsAlreadyDispatched = new ArrayList<>();
      this.deadline = deadline;
      this.blocking = blocking;
      this.uuid = UUID.randomUUID();
      this.datanodeRegistration = datanodeRegistration;
      this.storageId = storageId;
      this.datanodeUuid = datanodeUuid;
      this.allowPartialCompletion = allowPartialCompletion;
      this.partiallyScheduledBlocks = new HashSet<>();
      this.loggedMissed = false;
      this.averageBlockBytes = calculateAverageBlockSize(this.blockSets);
    }

    // Deep copy constructor (except for blockSets)
    private Request(Request other) {
      this.declaredAt = other.declaredAt;
      this.ioDeclarationType = other.getIoDeclarationType();
      this.callback = other.callback;
      this.blockSetsNeeded = other.getBlockSetsNeeded();
      this.bytesNeeded = other.bytesNeeded;
      this.scheduleByBytes = other.scheduleByBytes;
      this.blockSetsSelected = new ArrayList<>(other.getBlockSetsSelected());
      this.blockSetsAlreadyDispatched = other.getBlockSetsAlreadyDispatched();
      this.deadline = other.getDeadline();
      this.blocking = other.isBlocking();
      this.uuid = other.getUuid();
      this.datanodeRegistration = other.getDatanodeRegistration();
      this.storageId = other.getStorageId();
      this.datanodeUuid = other.getDatanodeUuid();
      this.allowPartialCompletion = other.doesAllowPartialCompletion();
      this.partiallyScheduledBlocks = new HashSet<>(other.getPartiallyScheduledBlocks());
      this.loggedMissed = other.isLoggedMissed();
      // Don't deep copy here since shouldn't be modified
      this.blockSets = other.getBlockSets();
      this.averageBlockBytes = calculateAverageBlockSize(this.blockSets);
    }

    public boolean scheduleByBytes() { return scheduleByBytes; }

    public long getBytesNeeded() { return bytesNeeded; }

    public void setBytesNeeded(long bytesNeeded) { this.bytesNeeded = bytesNeeded; }

    public Request getScheduledCopy() {
      return new Request(this);
    }

    public boolean isMissed() {
      return deadline.getTime() - System.currentTimeMillis() < 0;
    }

    public void setLoggedMissed() {
      LOG.info("[IO Planner] deadline missed for {}", this);
      loggedMissed = true;
    }

    public boolean isLoggedMissed() {
      return loggedMissed;
    }

    public boolean isComplete() {
      return callback.isDone();
    }

    public void complete(long blocksCached) {
      callback.complete(blocksCached);
    }

    public List<Set<LocatedBlock>> getBlockSets() {
      return blockSets;
    }

    public long getBlockSetsNeeded() { return blockSetsNeeded; }

    public void setBlockSetsNeeded(long newBlockSetsNeeded) { blockSetsNeeded = newBlockSetsNeeded; }

    public List<Long> getBlockSetsSelected() {
      return blockSetsSelected;
    }

    public List<Long> getBlockSetsAlreadyDispatched() {
      return blockSetsAlreadyDispatched;
    }

    public List<Long> getBlockSetsAlreadyScheduled() {
      List<Long> combined = new ArrayList<>(blockSetsAlreadyDispatched);
      combined.addAll(blockSetsSelected);
      return combined;
    }

    public String getIoDeclarationType() { return ioDeclarationType; }

    public Date getDeadline() {
      return deadline;
    }

    public UUID getUuid() { return uuid; }

    public boolean isBlocking() { return blocking; }

    public String getStorageId() { return storageId; }

    public String getDatanodeUuid() { return datanodeUuid; }

    public DatanodeRegistration getDatanodeRegistration() { return datanodeRegistration; }

    public boolean doesAllowPartialCompletion() { return allowPartialCompletion; }

    // Returns an estimate for how many bytes per second are needed to complete this request by the deadline
    public double getRateForCompletion(long currentTimeMillis) {
      long secondsToDeadline = (deadline.getTime() - currentTimeMillis) / 1000;
      if (secondsToDeadline < 0) {
        // Deadline is missed
        return Integer.MAX_VALUE;
      }
      if (blockSets.isEmpty()) return 0;
      if (blockSets.get(0).isEmpty()) return 0;
      if (this.allowPartialCompletion) {
        long blocksNeeded = blockSets.size() == 1 ? blockSetsNeeded : blockSetsNeeded * blockSets.get(0).size();
        return (double) blocksNeeded * averageBlockBytes / (double) secondsToDeadline;
      } else {
        // Assuming that all block sets have the same number of blocks
        long blocksNeeded = blockSetsNeeded * blockSets.get(0).size();
        return (double) blocksNeeded * averageBlockBytes / (double) secondsToDeadline;
      }
    }

    public long estimateBytesNeeded() {
      if (blockSets.isEmpty()) return 0;
      if (blockSets.get(0).isEmpty()) return 0;
      if (this.allowPartialCompletion && blockSets.size() == 1) {
        return blockSetsNeeded * averageBlockBytes;
      } else {
        // Assuming that all block sets have the same number of blocks
        return blockSetsNeeded * blockSets.get(0).size() * averageBlockBytes;
      }
    }

    public double estimateConstraint() {
      if (blockSets.isEmpty()) return 0;
      if (blockSets.get(0).isEmpty()) return 0;
      if (allowPartialCompletion && blockSets.size() == 1) {
        return (double) 1 / blockSets.get(0).size();
      } else {
        return (double) blockSetsNeeded / blockSets.size();
      }
    }

    public void clearPartiallyScheduledBlocks() {
      blockSetsSelected.clear();
      partiallyScheduledBlocks.clear();
    }

    public void addPartiallyScheduledBlocks(Set<LocatedBlock> newBlocks) {
      partiallyScheduledBlocks.addAll(newBlocks);
    }

    public Set<LocatedBlock> getPartiallyScheduledBlocks() { return partiallyScheduledBlocks; }

    @Override
    public String toString() {
      return "{ " + this.uuid + ", " + ioDeclarationType + ", requires " + this.blockSetsNeeded + "/" +
          (this.allowPartialCompletion ? blockSets.get(0).size() + " blocks" : this.blockSets.size() + " block sets" )
          + " } declared at " + this.declaredAt.toString() + ", deadline at " + this.deadline.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != getClass()) return false;
      Request other = (Request) o;
      return this.uuid.equals(other.getUuid());
    }

    @Override
    public int hashCode() {
      return this.uuid.hashCode();
    }
  }

  private static class Metrics {
    private final ConcurrentHashMap<String, AtomicLong> declaredBlocksPerTask;
    private final ConcurrentHashMap<String, AtomicLong> scheduledBlocksPerTask;
    private final ConcurrentHashMap<String, AtomicLong> scheduledBytesPerTask;
    private final ConcurrentHashMap<String, AtomicLong> freeBlocksPerTask;
    private final ConcurrentHashMap<String, AtomicLong> freeBytesPerTask;
    private final ConcurrentHashMap<String, AtomicLong> declarationsPerTask;
    private final ConcurrentHashMap<String, AtomicLong> scheduledRequestsPerTask;
    private final ConcurrentHashMap<String, AtomicLong> deadlinesMissedPerTask;
    private final ConcurrentHashMap<String, AtomicLong> requestsInQueuePerTask;

    private final AtomicLong totalRequests;
    private final AtomicLong deadlinesMissed;
    private final AtomicLong totalBlocksScheduled;
    private final AtomicLong totalBytesScheduled;
    private final AtomicLong totalFreeBlocksScheduled;
    private final AtomicLong totalFreeBytesScheduled;
    private final AtomicLong requestsInQueue;
    private final AtomicLong scheduledRequestsPending;

    private final ConcurrentHashMap<String, Map<Long, AtomicLong>> blockDeclarationCounts;
    private final ConcurrentHashMap<String, List<List<Date>>> blockIntervals;
    private final ConcurrentHashMap<String, Long> blockSizes;

    private final ConcurrentHashMap<String, AtomicLong> pairwiseOverlap;

    private final AtomicLong deletedBytes;
    private final AtomicLong deletedBlockCount;

    public Metrics() {
      totalBlocksScheduled = new AtomicLong(0);
      totalBytesScheduled = new AtomicLong(0);
      totalFreeBlocksScheduled = new AtomicLong(0);
      totalFreeBytesScheduled = new AtomicLong(0);
      declaredBlocksPerTask = new ConcurrentHashMap<>();
      scheduledBlocksPerTask = new ConcurrentHashMap<>();
      scheduledBytesPerTask = new ConcurrentHashMap<>();
      freeBlocksPerTask = new ConcurrentHashMap<>();
      freeBytesPerTask = new ConcurrentHashMap<>();
      totalRequests = new AtomicLong(0);
      deadlinesMissed = new AtomicLong(0);
      declarationsPerTask = new ConcurrentHashMap<>();
      scheduledRequestsPerTask = new ConcurrentHashMap<>();
      requestsInQueue = new AtomicLong(0);
      scheduledRequestsPending = new AtomicLong(0);
      blockDeclarationCounts = new ConcurrentHashMap<>();
      blockIntervals = new ConcurrentHashMap<>();
      blockSizes = new ConcurrentHashMap<>();
      deadlinesMissedPerTask = new ConcurrentHashMap<>();
      requestsInQueuePerTask = new ConcurrentHashMap<>();
      pairwiseOverlap = new ConcurrentHashMap<>();
      deletedBytes = new AtomicLong(0);
      deletedBlockCount = new AtomicLong(0);
    }

    public void addDeletedBytes(long bytes) {
      deletedBytes.getAndAdd(bytes);
      deletedBlockCount.getAndIncrement();
    }

    public void addPairwiseOverlap(String key, long bytes) {
      if (!pairwiseOverlap.containsKey(key)) {
        pairwiseOverlap.put(key, new AtomicLong(0));
      }
      pairwiseOverlap.get(key).getAndAdd(bytes);
    }

    public void printPairwiseOverlaps() {
      for (Map.Entry<String, AtomicLong> me : pairwiseOverlap.entrySet()) {
        LOG.info("[IOPlanner] Pairwise {}: {}", me.getKey(), me.getValue().get());
      }
    }

    public void declarationMetrics(Request request) {
      String ioDeclarationType = request.getIoDeclarationType();
      if (!declarationsPerTask.containsKey(ioDeclarationType)) {
        declarationsPerTask.put(ioDeclarationType, new AtomicLong(0));
      }
      declarationsPerTask.get(ioDeclarationType).getAndIncrement();
      if (!declaredBlocksPerTask.containsKey(ioDeclarationType)) {
        declaredBlocksPerTask.put(ioDeclarationType, new AtomicLong(0));
      }
      long newBlocks = 0;
      for (Set<LocatedBlock> bs : request.getBlockSets()) {
        for (LocatedBlock lb : bs) {
          if (lb.getBlockSize() == Long.MAX_VALUE) {
            LOG.info("[IOPlanner] Found a block with bytes == Long.MAX_VALUE, setting to 0 for accounting");
            lb.getBlock().setNumBytes(0);
          }
          for (String lid : lb.getStorageIDs()) {
            newBlocks++;
            long bid = lb.getBlock().getBlockId();
            if (BlockType.fromBlockId(bid) == BlockType.STRIPED) {
              bid = bid & ~BLOCK_GROUP_INDEX_MASK;
            }
            if (!blockDeclarationCounts.containsKey(lid)) {
              blockDeclarationCounts.put(lid, new HashMap<>());
            }
            Map<Long, AtomicLong> m = blockDeclarationCounts.get(lid);
            if (!m.containsKey(bid)) {
              m.put(bid, new AtomicLong(0));
            }
            m.get(bid).getAndIncrement();

            String intervalKey = lid + bid;
            if (!blockIntervals.containsKey(intervalKey)) {
              blockIntervals.put(intervalKey, new ArrayList<>());
            }
            if (!blockSizes.containsKey(intervalKey)) {
              blockSizes.put(intervalKey, lb.getBlockSize());
            }
            List<Date> interval = Arrays.asList(new Date(), request.getDeadline());
            blockIntervals.get(intervalKey).add(interval);
          }
        }
      }
      declaredBlocksPerTask.get(ioDeclarationType).getAndAdd(newBlocks);
      if (newBlocks == 0) {
        LOG.info("[IOPlanner] Request {} has no blocks declared", request);
      }
      if (request.getBlockSetsNeeded() == 0) {
        LOG.info("[IOPlanner] Request {} has no block sets needed", request);
      }
      if (!requestsInQueuePerTask.containsKey(ioDeclarationType)) {
        requestsInQueuePerTask.put(ioDeclarationType, new AtomicLong(0));
      }
      requestsInQueuePerTask.get(ioDeclarationType).getAndIncrement();
    }

    public void missedDeadline(Request req) {
      String ioDeclarationType = req.getIoDeclarationType();
      if (!deadlinesMissedPerTask.containsKey(ioDeclarationType)) {
        deadlinesMissedPerTask.put(ioDeclarationType, new AtomicLong(0));
      }
      deadlinesMissedPerTask.get(ioDeclarationType).getAndIncrement();
      deadlinesMissed.getAndIncrement();
    }

    public void removedFromQueue(Request req) {
      if (requestsInQueuePerTask.containsKey(req.getIoDeclarationType())) {
        requestsInQueuePerTask.get(req.getIoDeclarationType()).getAndDecrement();
      }
    }

    public void scheduledQueueMetrics(long scheduledRequestsPending) { this.scheduledRequestsPending.set(scheduledRequestsPending); }

    public void queueMetrics(long requestsInQueue) {
      this.requestsInQueue.set(requestsInQueue);
    }

    public void recordScheduledRequest(Request req, long freeBlocks, long nonFreeBlocks, long freeB, long nonfreeB) {
      if (freeBlocks + nonFreeBlocks == 0) {
        LOG.info("[IOPlanner] Request {} was scheduled with no blocks selected", req);
      }
      if (freeB + nonfreeB == 0) {
        LOG.info("[IOPlanner] Request {} was scheduled with no bytes", req);
      }
      
      // Note that the overlap stats are not necessarily reflected in dispatch and that the total requests count
      // can double count requests if they are selected multiple times in a window
      String declarationType = req.getIoDeclarationType();

      if (!scheduledRequestsPerTask.containsKey(declarationType)) {
        scheduledRequestsPerTask.put(declarationType, new AtomicLong(0));
      }
      scheduledRequestsPerTask.get(declarationType).getAndIncrement();
      if (!scheduledBlocksPerTask.containsKey(declarationType)) {
        scheduledBlocksPerTask.put(declarationType, new AtomicLong(0));
      }
      scheduledBlocksPerTask.get(declarationType).getAndAdd(freeBlocks + nonFreeBlocks);
      if (!scheduledBytesPerTask.containsKey(declarationType)) {
        scheduledBytesPerTask.put(declarationType, new AtomicLong(0));
      }
      scheduledBytesPerTask.get(declarationType).getAndAdd(freeB + nonfreeB);
      if (!freeBlocksPerTask.containsKey(declarationType)) {
        freeBlocksPerTask.put(declarationType, new AtomicLong(0));
      }
      freeBlocksPerTask.get(declarationType).getAndAdd(freeBlocks);
      if (!freeBytesPerTask.containsKey(declarationType)) {
        freeBytesPerTask.put(declarationType, new AtomicLong(0));
      }
      freeBytesPerTask.get(declarationType).getAndAdd(freeB);

      totalRequests.getAndIncrement();
      totalBlocksScheduled.getAndAdd(freeBlocks + nonFreeBlocks);
      totalBytesScheduled.getAndAdd(freeB + nonfreeB);
      totalFreeBlocksScheduled.getAndAdd(freeBlocks);
      totalFreeBytesScheduled.getAndAdd(freeB);
    }

    public void printTotalsToLogCSV() {
      LOG.info("[IO Planner] Declarations scheduled = {}, blocks scheduled = {}, " +
              "free blocks scheduled = {}, deadlines missed = {}, declarations in queue = {}, scheduled declaration queue = {}",
          totalRequests.longValue(), totalBlocksScheduled.longValue(), totalFreeBlocksScheduled.longValue(),
          deadlinesMissed.longValue(), requestsInQueue.longValue(), scheduledRequestsPending.longValue());
      LOG.info("[IO Planner] Bytes scheduled = {}, free bytes scheduled = {}, bytes deleted = {}, blocks deleted = {}",
          totalBytesScheduled.longValue(), totalFreeBytesScheduled.longValue(), deletedBytes.longValue(), deletedBlockCount.longValue());
    }

    public void printTaskTotalsToLogCSV() {
      for (String key : declarationsPerTask.keySet()) {
        LOG.info("[IO Planner] {}: declarations = {}, declared blocks = {}, " +
                "declarations scheduled = {}, blocks scheduled = {}, free blocks scheduled = {}, " +
                "declarations in queue = {}, deadlines missed = {}",
            key, declarationsPerTask.get(key), declaredBlocksPerTask.getOrDefault(key, new AtomicLong()).get(),
            scheduledRequestsPerTask.getOrDefault(key, new AtomicLong()).get(), scheduledBlocksPerTask.getOrDefault(key, new AtomicLong()).get(),
            freeBlocksPerTask.getOrDefault(key, new AtomicLong()).get(), requestsInQueuePerTask.getOrDefault(key, new AtomicLong()).get(),
            deadlinesMissedPerTask.getOrDefault(key, new AtomicLong()).get());
        LOG.info("[IO Planner] {}: bytes scheduled = {}, free bytes scheduled = {}",
            key, scheduledBytesPerTask.getOrDefault(key, new AtomicLong()).get(), freeBytesPerTask.getOrDefault(key, new AtomicLong()).get());
      }
    }

    // Minimum calculation is an estimate because it includes all declared blocks even if you only need N%
    public void printMinimumBytesReadToLog() {
      long totalBytes = 0;
      for (Map.Entry<String, List<List<Date>>> me : blockIntervals.entrySet()) {
        List<List<Date>> intervals = me.getValue();
        if (intervals.isEmpty()) {
          continue;
        }
        Date[][] arr = new Date[intervals.size()][];
        for (int i = 0; i < intervals.size(); i++) {
          arr[i] = intervals.get(i).toArray(new Date[0]);
        }
        Arrays.sort(arr, Comparator.comparing(i -> i[1]));
        long minimumPoints = 1;
        Date curPoint = arr[0][1];
        for (int i = 1; i < intervals.size(); i++) {
          if (curPoint.before(arr[i][0])) {
            minimumPoints++;
            curPoint = arr[i][1];
          }
        }
        long blockBytes = blockSizes.getOrDefault(me.getKey(), 0L);
        if (blockBytes == 0L) {
          LOG.info("[IO Planner] block size is recorded as 0 bytes or not found, so removing");
          blockSizes.remove(me.getKey());
        }
        totalBytes += minimumPoints * blockBytes;
      }
      LOG.info("[IO Planner] With intervals, minimum read IO needed is {} MB", totalBytes / (1024 * 1024));
    }
  }

}
