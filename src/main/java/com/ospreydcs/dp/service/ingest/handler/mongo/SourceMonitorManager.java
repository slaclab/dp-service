package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.model.SourceMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class manages subscriptions made via the subscribeData() API. It publishes data received in the data ingestion
 * stream to subscribers registered by PV name.
 *
 * A SourceMonitor object is created for each PV subscription and added to the subscriptionMap.
 *
 * Concurrency mechanisms are provided to make this class thread safe.
 *
 * Methods are provided for adding, removing and terminating SourceMonitors, and for publishing ingested PV data
 * to subscribers.
 */
public class SourceMonitorManager {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final Map<String, List<SourceMonitor>> subscriptionMap = new HashMap<>();
    public final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


    public boolean init() {
        return true;
    }

    public boolean fini() {

        logger.debug("SourceMonitorManager fini");

        if (shutdownRequested.compareAndSet(false, true)) {

            // only acquire readLock to access local data structure, release before shutting down the monitors
            readLock.lock();

            Set<SourceMonitor> sourceMonitors = null;
            try {

                logger.debug("SourceMonitorManager fini shutting down SourceMonitors");

                // create a set of all SourceMonitors, eliminating duplicates for subscriptions to multiple PVs
                sourceMonitors = new HashSet<>();
                for (List<SourceMonitor> monitorList : subscriptionMap.values()) {
                    sourceMonitors.addAll(monitorList);
                }

            } finally {
                // release the lock after accessing data structure but before shutting down monitors
                readLock.unlock();
            }
            Objects.requireNonNull(sourceMonitors);

            // after releasing lock, close each response stream in set
            for (SourceMonitor monitor : sourceMonitors) {
                monitor.requestShutdown();
            }

        }

        return true;
    }

    /**
     * Add a subscription entry to map data structure.  We use a write lock for thread safety between calling threads
     * (e.g., threads handling registration of subscriptions).
     */
    public void addMonitor(SourceMonitor monitor) {

        if (shutdownRequested.get()) {
            return;
        }

        writeLock.lock();
        try {
            // use try...finally to make sure we unlock

            for (String pvName : monitor.pvNames) {
                List<SourceMonitor> sourceMonitors = subscriptionMap.computeIfAbsent(pvName, k -> new ArrayList<>());
                sourceMonitors.add(monitor);
            }

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Publish columns from ingestion request whose PVs have subscriptions.
     * We use a read lock for thread safety between calling threads (e.g., workers processing ingestion requests).
     *
     * @param request
     */
    public void publishDataSubscriptions(IngestDataRequest request, String providerName) {

        if (shutdownRequested.get()) {
            return;
        }

        final DataTimestamps requestDataTimestamps = request.getIngestionDataFrame().getDataTimestamps();

        // publish DataColumns in request that have subscribers
        for (DataColumn requestDataColumn : request.getIngestionDataFrame().getDataColumnsList()) {
            final String pvName = requestDataColumn.getName();
            final List<SourceMonitor> pvSubscribers = getSubscribersForPv(pvName);
            if (pvSubscribers.size() > 0) {
                // create DataBucket for column
                DataBucket columnBucket = DataBucket.newBuilder()
                        .setPvName(pvName)
                        .setDataTimestamps(requestDataTimestamps)
                        .setDataColumn(requestDataColumn)
                        .setProviderId(request.getProviderId())
                        .setProviderName(providerName)
                        .build();
                // publish DataBucket to each subscriber
                for (SourceMonitor monitor : pvSubscribers) {
                    monitor.publishDataBucket(pvName, columnBucket);
                }
            }
        }

        // publish SerializedDataColumns in request that have subscribers
        for (SerializedDataColumn requestColumn : request.getIngestionDataFrame().getSerializedDataColumnsList()) {
            final String pvName = requestColumn.getName();
            final List<SourceMonitor> pvSubscribers = getSubscribersForPv(pvName);
            if (pvSubscribers.size() > 0) {
                // create DataBucket for column
                DataBucket columnBucket = DataBucket.newBuilder()
                        .setPvName(pvName)
                        .setDataTimestamps(requestDataTimestamps)
                        .setSerializedDataColumn(requestColumn)
                        .setProviderId(request.getProviderId())
                        .setProviderName(providerName)
                        .build();
                // publish DataBucket to each subscriber
                for (SourceMonitor monitor : pvSubscribers) {
                    monitor.publishDataBucket(pvName, columnBucket);
                }
            }
        }

        // publish DoubleColumns in request that have subscribers
        for (DoubleColumn requestColumn : request.getIngestionDataFrame().getDoubleColumnsList()) {
            final String pvName = requestColumn.getName();
            final List<SourceMonitor> pvSubscribers = getSubscribersForPv(pvName);
            if (pvSubscribers.size() > 0) {
                // create DataBucket for column
                DataBucket columnBucket = DataBucket.newBuilder()
                        .setPvName(pvName)
                        .setDataTimestamps(requestDataTimestamps)
                        .setDoubleColumn(requestColumn)
                        .setProviderId(request.getProviderId())
                        .setProviderName(providerName)
                        .build();
                // publish DataBucket to each subscriber
                for (SourceMonitor monitor : pvSubscribers) {
                    monitor.publishDataBucket(pvName, columnBucket);
                }
            }
        }

        // publish FloatColumns in request that have subscribers
        for (FloatColumn requestColumn : request.getIngestionDataFrame().getFloatColumnsList()) {
            final String pvName = requestColumn.getName();
            final List<SourceMonitor> pvSubscribers = getSubscribersForPv(pvName);
            if (pvSubscribers.size() > 0) {
                // create DataBucket for column
                DataBucket columnBucket = DataBucket.newBuilder()
                        .setPvName(pvName)
                        .setDataTimestamps(requestDataTimestamps)
                        .setFloatColumn(requestColumn)
                        .setProviderId(request.getProviderId())
                        .setProviderName(providerName)
                        .build();
                // publish DataBucket to each subscriber
                for (SourceMonitor monitor : pvSubscribers) {
                    monitor.publishDataBucket(pvName, columnBucket);
                }
            }
        }

    }

    private List<SourceMonitor> getSubscribersForPv(String pvName) {
        // acquire readLock only long enough to read local data structure
        readLock.lock();
        List<SourceMonitor> sourceMonitorsCopy;
        try {
            final List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
            sourceMonitorsCopy =
                    (sourceMonitors == null) ? new ArrayList<>() : new ArrayList<>(sourceMonitors);
        } finally {
            readLock.unlock();
        }
        return sourceMonitorsCopy;
    }

    /**
     * Remove all subscriptions from map for specified SourceMonitor, and then request shutdown.
     * We use a write lock for thread safety between calling threads
     * (e.g., threads handling registration of subscriptions).
     */
    public void removeMonitor(SourceMonitor monitor) {

        if (shutdownRequested.get()) {
            return;
        }

        writeLock.lock();
        try {
            // use try...finally to make sure we unlock

            for (String pvName : monitor.pvNames) {
                List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
                if (sourceMonitors != null) {
                    logger.debug(
                            "removing subscription for id: {} pv: {}",
                            monitor.responseObserver.hashCode(), pvName);
                    sourceMonitors.remove(monitor);
                }
            }

        } finally {
            writeLock.unlock();
        }
    }

    public void terminateMonitor(SourceMonitor monitor) {

        if (shutdownRequested.get()) {
            return;
        }

        logger.debug("terminateMonitor id: {}", monitor.responseObserver.hashCode());

        // terminate the SourceMonitor first (before acquiring any locks)
        monitor.requestShutdown();

        // then remove SourceMonitor from local data structures
        this.removeMonitor(monitor);
    }
}
