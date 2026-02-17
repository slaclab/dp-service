package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class manages buffered data for a single target PV associated with a subscribeDataEvent() handler's
 * EventMonitor.  It maintains a list of BufferedDataItems, with the policy for aging and flushing buffered data
 * specified by the DataBufferConfig.
 *
 * Each BufferedDataItem contains a protobuf column data type message received via the subscribeData() API response
 * stream, along with the corresponding DataTimestamps message specifying the timestamps for the column data values.
 * Each item also includes a timestamp for use in aging buffered data, and an estimated size in bytes for use in
 * checking response stream message size limits.
 */
public class DataBuffer {

    private static final Logger logger = LogManager.getLogger();

    private final String pvName;
    private final DataBufferConfig config;
    private final List<BufferedDataItem> bufferedItems = new ArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    
    private long currentBufferSizeBytes = 0;
    private Instant lastFlushTime = Instant.now();

    /**
     * Specifies the details for aging and flushing the DataBuffer 1) at a specified time interval, 2) at a maximum
     * buffer size in bytes, 3) at a maximum buffer size in items, and 4) a maximum item age in nanoseconds.
     */
    public static class DataBufferConfig {
        private final long flushIntervalMs;
        private final long maxBufferSizeBytes;
        private final int maxBufferItems;
        private final long maxItemAgeNanos;

        public DataBufferConfig(long flushIntervalMs, long maxBufferSizeBytes, int maxBufferItems, long maxItemAgeNanos) {
            this.flushIntervalMs = flushIntervalMs;
            this.maxBufferSizeBytes = maxBufferSizeBytes;
            this.maxBufferItems = maxBufferItems;
            this.maxItemAgeNanos = maxItemAgeNanos;
        }

        public long getFlushIntervalMs() { return flushIntervalMs; }
        public long getMaxBufferSizeBytes() { return maxBufferSizeBytes; }
        public int getMaxBufferItems() { return maxBufferItems; }
        public long getMaxItemAgeNanos() { return maxItemAgeNanos; }

        // Convenience methods for time conversion
        public static long millisecondsToNanos(long milliseconds) {
            return milliseconds * 1_000_000L;
        }

        public static long secondsToNanos(long seconds) {
            return seconds * 1_000_000_000L;
        }
    }

    /**
     * Encapsulates a PV column data vector in the DataBuffer using ProtobufColumnType to specify the column data type
     * and protobufColumn to contain the protobuf column message.  Includes dataTimestamps for use in determining if
     * the buffered data overlaps the time window of a trigered event, a timestamp for aging the item in the buffer,
     * and an estimated size for use in checking response stream message size limites.
     */
    private static class BufferedDataItem {

        private final EventMonitor.ProtobufColumnType protobufColumnType;
        private final Object protobufColumn;
        private final DataTimestamps dataTimestamps;
        private final Instant timestamp;
        private final long estimatedSizeBytes;

        private BufferedDataItem(
                EventMonitor.ProtobufColumnType protobufColumnType,
                Object protobufColumn,
                DataTimestamps dataTimestamps,
                long estimatedSizeBytes
        ) {
            this.protobufColumnType = protobufColumnType;
            this.protobufColumn = protobufColumn;
            this.dataTimestamps = dataTimestamps;
            this.estimatedSizeBytes = estimatedSizeBytes;
            this.timestamp = Instant.now();
        }

        public EventMonitor.ProtobufColumnType getProtobufColumnType() { return protobufColumnType; }
        public Object getProtobufColumn() { return protobufColumn; }
        public DataTimestamps getDataTimestamps() { return dataTimestamps; }
        public Instant getTimestamp() { return timestamp; }
        public long getEstimatedSizeBytes() { return estimatedSizeBytes; }
    }

    /**
     * Used to deliver BufferedDataItems flushed from the buffer via the DataBufferManager's DataProcessor interface
     * to the consumer of flushed data for dispatching in the subscribeDataEvent() response stream.
     */
    public static class BufferedData {

        private final EventMonitor.ProtobufColumnType protobufColumnType;
        private final Object protobufColumn;
        private final DataTimestamps dataTimestamps;
        private final long estimatedSize;
        private final Instant firstInstant;
        private final Instant lastInstant;

        public BufferedData(BufferedDataItem bufferedDataItem) {

            this.protobufColumnType = bufferedDataItem.protobufColumnType;
            this.protobufColumn = bufferedDataItem.protobufColumn;
            this.dataTimestamps = bufferedDataItem.getDataTimestamps();
            this.estimatedSize = bufferedDataItem.getEstimatedSizeBytes();

            // set begin / end times from dataTimestamps
            final DataTimestampsUtility.DataTimestampsModel dataTimestampsModel =
                    new DataTimestampsUtility.DataTimestampsModel(dataTimestamps);
            firstInstant = TimestampUtility.instantFromTimestamp(dataTimestampsModel.getFirstTimestamp());
            lastInstant = TimestampUtility.instantFromTimestamp(dataTimestampsModel.getLastTimestamp());
        }

        public EventMonitor.ProtobufColumnType getProtobufColumnType() { return protobufColumnType; }
        public Object getProtobufColumn() { return protobufColumn; }
        public DataTimestamps getDataTimestamps() { return dataTimestamps; }
        public long getEstimatedSize() { return estimatedSize; }
        public Instant getFirstInstant() { return firstInstant; }
        public Instant getLastInstant() { return lastInstant; }
    }

    public DataBuffer(String pvName, DataBufferConfig config) {
        this.pvName = pvName;
        this.config = config;
    }

    /**
     * Adds protobuf column data to the DataBuffer's list of items.
     *
     * @param protobufColumnType
     * @param protobufColumn
     * @param dataTimestamps
     */
    public void addData(
            EventMonitor.ProtobufColumnType protobufColumnType,
            Object protobufColumn,
            DataTimestamps dataTimestamps
    ) {
        writeLock.lock();
        try {
            long estimatedSize = estimateDataSize(protobufColumnType, protobufColumn);
            BufferedDataItem item =
                    new BufferedDataItem(protobufColumnType, protobufColumn, dataTimestamps, estimatedSize);
            
            bufferedItems.add(item);
            currentBufferSizeBytes += estimatedSize;
            
            logger.debug("Added DataColumn to buffer for PV: {}, buffer size: {} bytes, {} items",
                        pvName, currentBufferSizeBytes, bufferedItems.size());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Determines when its time to flush the DataBuffer by checking if 1) items have exceeded the maximum age,
     * 2) if the flush time interval has passed, 3) if the buffer size limit in bytes or number of items is surpassed.
     *
     * @return
     */
    public boolean shouldFlush() {
        readLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return false;
            }

            Instant now = Instant.now();
            long timeSinceLastFlush = now.toEpochMilli() - lastFlushTime.toEpochMilli();
            
            // Check if any items have exceeded max age
            boolean hasExpiredItems = bufferedItems.stream()
                .anyMatch(item -> {
                    Duration itemAge = Duration.between(item.getTimestamp(), now);
                    return itemAge.toNanos() >= config.getMaxItemAgeNanos();
                });
            
            return timeSinceLastFlush >= config.getFlushIntervalMs() ||
                   currentBufferSizeBytes >= config.getMaxBufferSizeBytes() ||
                   bufferedItems.size() >= config.getMaxBufferItems() ||
                   hasExpiredItems;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Flushes and returns items that have reached the configured age from the DataBuffer.
     *
     * @return
     */
    public List<BufferedData> flush() {
        writeLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return new ArrayList<>();
            }

            Instant now = Instant.now();
            List<BufferedData> results = new ArrayList<>();
            List<BufferedDataItem> itemsToRemove = new ArrayList<>();
            
            // Only flush items that have reached the configured age
            for (BufferedDataItem item : bufferedItems) {
                Duration itemAge = Duration.between(item.getTimestamp(), now);
                if (itemAge.toNanos() >= config.getMaxItemAgeNanos()) {
                    results.add(new BufferedData(item));
                    itemsToRemove.add(item);
                }
            }

            // Remove flushed items and update buffer size
            for (BufferedDataItem item : itemsToRemove) {
                bufferedItems.remove(item);
                currentBufferSizeBytes -= item.getEstimatedSizeBytes();
            }

            if (!results.isEmpty()) {
                logger.debug("Flushing {} aged items from buffer for PV: {}, {} items remaining, {} bytes remaining", 
                            results.size(), pvName, bufferedItems.size(), currentBufferSizeBytes);
            }

            lastFlushTime = now;
            return results;
        } finally {
            writeLock.unlock();
        }
    }

    public int getBufferedItemCount() {
        readLock.lock();
        try {
            return bufferedItems.size();
        } finally {
            readLock.unlock();
        }
    }

    public long getCurrentBufferSizeBytes() {
        readLock.lock();
        try {
            return currentBufferSizeBytes;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Estimates the message size in bytes for the supplied protobufColumn, using the column data type to determine
     * the estimate.
     *
     * @param protobufColumnType
     * @param protobufColumn
     * @return
     */
    private long estimateDataSize(
            EventMonitor.ProtobufColumnType protobufColumnType,
            Object protobufColumn
    ) {
        AtomicLong size = new AtomicLong(100); // Base overhead for timestamps and structure

        switch (protobufColumnType) {

            case DATA_COLUMN -> { // protobufColumn is an instance of DataColumn
                if (protobufColumn instanceof DataColumn) {
                    final DataColumn dataColumn = (DataColumn) protobufColumn;
                    size.addAndGet(dataColumn.getName().length() * 2); // String overhead
                    size.addAndGet(dataColumn.getDataValuesList().size() * 50); // Base per-value overhead
                    dataColumn.getDataValuesList().forEach(dataValue -> {
                        switch (dataValue.getValueCase()) {
                            case STRINGVALUE:
                                size.addAndGet(dataValue.getStringValue().length() * 2);
                                break;
                            case BYTEARRAYVALUE:
                                size.addAndGet(dataValue.getByteArrayValue().size());
                                break;
                            case ARRAYVALUE:
                                size.addAndGet(dataValue.getArrayValue().getDataValuesCount() * 32);
                                break;
                            case STRUCTUREVALUE:
                                size.addAndGet(dataValue.getStructureValue().getFieldsCount() * 64);
                                break;
                            case IMAGEVALUE:
                                size.addAndGet(dataValue.getImageValue().getImage().size());
                                break;
                            default:
                                size.addAndGet(8); // Primitive types
                                break;
                        }
                    });
                }
            }

            case SERIALIZED_DATA_COLUMN -> {
                if (protobufColumn instanceof SerializedDataColumn) {
                    final SerializedDataColumn serializedDataColumn = (SerializedDataColumn) protobufColumn;
                    size.addAndGet(serializedDataColumn.getName().length() * 2); // String overhead
                    size.addAndGet(50); // Base overhead for data column bytes.
                    size.addAndGet(serializedDataColumn.getSerializedSize());
                }
            }

            case DOUBLE_COLUMN -> {
                if (protobufColumn instanceof DoubleColumn) {
                    final DoubleColumn doubleColumn = (DoubleColumn) protobufColumn;
                    size.addAndGet(doubleColumn.getName().length() * 2);
                    size.addAndGet(doubleColumn.getValuesCount() * 8); // number of list elements * primitive size
                }
            }
        }

        return size.get();
    }

    public List<BufferedData> forceFlushAll() {
        writeLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return new ArrayList<>();
            }

            List<BufferedData> results = new ArrayList<>();
            for (BufferedDataItem item : bufferedItems) {
                results.add(new BufferedData(item));
            }

            logger.debug("Force flushing all {} items from buffer for PV: {}, {} bytes", 
                        bufferedItems.size(), pvName, currentBufferSizeBytes);

            bufferedItems.clear();
            currentBufferSizeBytes = 0;
            lastFlushTime = Instant.now();

            return results;
        } finally {
            writeLock.unlock();
        }
    }

    public int getItemsReadyToFlush() {
        readLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return 0;
            }

            Instant now = Instant.now();
            return (int) bufferedItems.stream()
                .filter(item -> {
                    Duration itemAge = Duration.between(item.getTimestamp(), now);
                    return itemAge.toNanos() >= config.getMaxItemAgeNanos();
                })
                .count();
        } finally {
            readLock.unlock();
        }
    }
}