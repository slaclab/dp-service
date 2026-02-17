package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to buffer target PV data for the EventMonitor created to manage a subscribeDataEvent()
 * subscription.
 *
 * It uses a DataBuffer for each target PV specified in the subscription, with the pvBuffers map containing an entry
 * for each.  It uses the flushScheduler thread pool to manage flushing the data buffers at the configured interval.
 * It defines the DataProcessor interface, which is implemented by the client EventMonitor to receive data flushed
 * from buffers.
 */
public class DataBufferManager {

    private static final Logger logger = LogManager.getLogger();

    @FunctionalInterface
    public interface DataProcessor {
        void processData(String pvName, List<DataBuffer.BufferedData> results);
    }

    private final DataBuffer.DataBufferConfig config;
    private final Map<String, DataBuffer> pvBuffers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService flushScheduler = Executors.newScheduledThreadPool(2);
    private final DataProcessor dataProcessor;

    public DataBufferManager(DataProcessor dataProcessor, DataBuffer.DataBufferConfig config) {
        this.dataProcessor = dataProcessor;
        this.config = config;
        startPeriodicFlush();
    }

    public void bufferData(
            String pvName,
            EventMonitor.ProtobufColumnType protobufColumnType,
            Object protobufColumn,
            DataTimestamps dataTimestamps
    ) {
        DataBuffer buffer = pvBuffers.computeIfAbsent(pvName, k -> new DataBuffer(k, config));
        buffer.addData(protobufColumnType, protobufColumn, dataTimestamps);
        
        if (buffer.shouldFlush()) {
            flushBuffer(pvName, buffer);
        }
    }

    public void forceFlushAll() {
        pvBuffers.forEach(this::flushBuffer);
    }

    public void forceFlushAllItems() {
        pvBuffers.forEach((pvName, buffer) -> {
            try {
                List<DataBuffer.BufferedData> results = buffer.forceFlushAll();
                if (!results.isEmpty()) {
                    processBufferedResults(pvName, results);
                }
            } catch (Exception e) {
                logger.error("Error force flushing all items for PV: {}", pvName, e);
            }
        });
    }

    public void removePvBuffer(String pvName) {
        DataBuffer buffer = pvBuffers.remove(pvName);
        if (buffer != null) {
            flushBuffer(pvName, buffer);
        }
    }

    public void shutdown() {
        logger.info("Shutting down DataBufferManager");
        
        forceFlushAll();
        
        flushScheduler.shutdown();
        try {
            if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                flushScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void startPeriodicFlush() {
        long flushCheckInterval = Math.max(config.getFlushIntervalMs() / 4, 100);
        
        flushScheduler.scheduleWithFixedDelay(this::checkAndFlushBuffers, 
                                            flushCheckInterval, 
                                            flushCheckInterval, 
                                            TimeUnit.MILLISECONDS);
    }

    private void checkAndFlushBuffers() {
        try {
            pvBuffers.forEach((pvName, buffer) -> {
                if (buffer.shouldFlush()) {
                    flushBuffer(pvName, buffer);
                }
            });
        } catch (Exception e) {
            logger.error("Error during periodic buffer flush check", e);
        }
    }

    private void flushBuffer(String pvName, DataBuffer buffer) {
        try {
            List<DataBuffer.BufferedData> results = buffer.flush();
            if (!results.isEmpty()) {
                processBufferedResults(pvName, results);
            }
        } catch (Exception e) {
            logger.error("Error flushing buffer for PV: {}", pvName, e);
        }
    }

    private void processBufferedResults(String pvName, List<DataBuffer.BufferedData> results) {
        dataProcessor.processData(pvName, results);
    }

    public int getTotalBufferedItems() {
        return pvBuffers.values().stream()
                       .mapToInt(DataBuffer::getBufferedItemCount)
                       .sum();
    }

    public long getTotalBufferedBytes() {
        return pvBuffers.values().stream()
                       .mapToLong(DataBuffer::getCurrentBufferSizeBytes)
                       .sum();
    }

    public int getBufferCount() {
        return pvBuffers.size();
    }

    public int getTotalItemsReadyToFlush() {
        return pvBuffers.values().stream()
                       .mapToInt(DataBuffer::getItemsReadyToFlush)
                       .sum();
    }
}