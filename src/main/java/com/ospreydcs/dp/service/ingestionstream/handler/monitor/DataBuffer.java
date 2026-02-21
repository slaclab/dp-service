package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.common.*;
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
 * Each BufferedDataItem contains a protobuf DataBucket message received via the subscribeData() API response
 * stream. Each item also includes a timestamp for use in aging buffered data, and an estimated size in bytes for use in
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
     * Encapsulates a DataBucket in the DataBuffer.  Uses the bucket's dataTimestamps for determining if
     * the buffered data overlaps the time window of a triggered event.  Includes a timestamp for aging the item in the
     * buffer, and an estimated size for use in checking response stream message size limits.
     */
    private static class BufferedDataItem {

        private final DataBucket dataBucket;
        private final Instant timestamp;
        private final long estimatedSizeBytes;

        private BufferedDataItem(
                DataBucket dataBucket,
                long estimatedSizeBytes
        ) {
            this.dataBucket = dataBucket;
            this.estimatedSizeBytes = estimatedSizeBytes;
            this.timestamp = Instant.now();
        }

        public DataBucket getDataBucket() { return dataBucket; }
        public Instant getTimestamp() { return timestamp; }
        public long getEstimatedSizeBytes() { return estimatedSizeBytes; }
    }

    /**
     * Used to deliver BufferedDataItems flushed from the buffer via the DataBufferManager's DataProcessor interface
     * to the consumer of flushed data for dispatching in the subscribeDataEvent() response stream.
     */
    public static class BufferedData {

        private final DataBucket dataBucket;
        private final long estimatedSize;
        private final Instant firstInstant;
        private final Instant lastInstant;

        public BufferedData(BufferedDataItem bufferedDataItem) {

            this.dataBucket = bufferedDataItem.getDataBucket();
            this.estimatedSize = bufferedDataItem.getEstimatedSizeBytes();

            // set begin / end times from dataTimestamps
            final DataTimestampsUtility.DataTimestampsModel dataTimestampsModel =
                    new DataTimestampsUtility.DataTimestampsModel(dataBucket.getDataTimestamps());
            firstInstant = TimestampUtility.instantFromTimestamp(dataTimestampsModel.getFirstTimestamp());
            lastInstant = TimestampUtility.instantFromTimestamp(dataTimestampsModel.getLastTimestamp());
        }

        public DataBucket getDataBucket() { return dataBucket; }
        public long getEstimatedSize() { return estimatedSize; }
        public Instant getFirstInstant() { return firstInstant; }
        public Instant getLastInstant() { return lastInstant; }
    }

    public DataBuffer(String pvName, DataBufferConfig config) {
        this.pvName = pvName;
        this.config = config;
    }

    /**
     * Adds entry for dataBucket to the DataBuffer's list of items.
     */
    public void addData(
            DataBucket dataBucket
    ) {
        writeLock.lock();
        try {
            // estimate size for DataBucket
            long estimatedSize = estimateDataSize(dataBucket);
            if (estimatedSize == 0L) {
                logger.error(
                        "DataBuffer.estimateDataSize() returned zero for dataBucket.dataCase: "
                                + dataBucket.getDataCase());
            }

            BufferedDataItem item = new BufferedDataItem(dataBucket, estimatedSize);
            
            bufferedItems.add(item);
            currentBufferSizeBytes += estimatedSize;
            
            logger.debug("Added DataColumn to buffer for PV: {}, buffer size: {} bytes, {} items",
                        pvName, currentBufferSizeBytes, bufferedItems.size());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Determines when it's time to flush the DataBuffer by checking if 1) items have exceeded the maximum age,
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
     * Estimates the message size in bytes for the supplied dataBucket, using the column data type to determine
     * the estimate.
     */
    private long estimateDataSize(
            DataBucket dataBucket
    ) {
        AtomicLong size = new AtomicLong(100); // Base overhead for timestamps and structure
        switch(dataBucket.getDataCase()) {
            case DATACOLUMN -> {
                final DataColumn dataColumn = dataBucket.getDataColumn();
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
            case SERIALIZEDDATACOLUMN -> {
                final SerializedDataColumn serializedDataColumn = dataBucket.getSerializedDataColumn();
                size.addAndGet(serializedDataColumn.getName().length() * 2); // String overhead
                size.addAndGet(50); // Base overhead for data column bytes.
                size.addAndGet(serializedDataColumn.getSerializedSize());
            }
            case DOUBLECOLUMN -> {
                final DoubleColumn doubleColumn = dataBucket.getDoubleColumn();
                size.addAndGet(doubleColumn.getName().length() * 2);
                size.addAndGet(doubleColumn.getValuesCount() * 8); // number of list elements * primitive size
            }
            case FLOATCOLUMN -> {
                final FloatColumn floatColumn = dataBucket.getFloatColumn();
                size.addAndGet(floatColumn.getName().length() * 2);
                size.addAndGet(floatColumn.getValuesCount() * 4); // number of list elements * primitive size
            }
            case INT64COLUMN -> {
                final Int64Column int64Column = dataBucket.getInt64Column();
                size.addAndGet(int64Column.getName().length() * 2);
                size.addAndGet(int64Column.getValuesCount() * 8); // 8 bytes per long
            }
            case INT32COLUMN -> {
                final Int32Column int32Column = dataBucket.getInt32Column();
                size.addAndGet(int32Column.getName().length() * 2);
                size.addAndGet(int32Column.getValuesCount() * 4); // 4 bytes per int
            }
            case BOOLCOLUMN -> {
                final BoolColumn boolColumn = dataBucket.getBoolColumn();
                size.addAndGet(boolColumn.getName().length() * 2);
                size.addAndGet(boolColumn.getValuesCount() * 1); // 1 byte per boolean
            }
            case STRINGCOLUMN -> {
                final StringColumn stringColumn = dataBucket.getStringColumn();
                size.addAndGet(stringColumn.getName().length() * 2);
                for (String value : stringColumn.getValuesList()) {
                    size.addAndGet(value.length() * 2); // 2 bytes per character (UTF-16)
                }
            }
            case ENUMCOLUMN -> {
                final EnumColumn enumColumn = dataBucket.getEnumColumn();
                size.addAndGet(enumColumn.getName().length() * 2);
                size.addAndGet(enumColumn.getEnumId().length() * 2);
                size.addAndGet(enumColumn.getValuesCount() * 4); // 4 bytes per int32
            }
            case IMAGECOLUMN -> {
            }
            case STRUCTCOLUMN -> {
            }
            case DOUBLEARRAYCOLUMN -> {
            }
            case FLOATARRAYCOLUMN -> {
            }
            case INT32ARRAYCOLUMN -> {
            }
            case INT64ARRAYCOLUMN -> {
            }
            case BOOLARRAYCOLUMN -> {
            }
            case DATA_NOT_SET -> {
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